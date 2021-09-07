package workflow

import (
	"context"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	cou "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/regs"
	"sort"
	"sync"
	"time"
)

type SaverStats struct {
	ItemsCounter regs.Counter
}

type DeviceTypeSaverStats struct {
	DTStats               map[DeviceType]*SaverStats
	StartTime, FinishTime time.Time
}

type Saver interface {
	ErrCh() <-chan errs.Error
	Done() <-chan struct{}
	Run(ctx context.Context, dtInputs map[DeviceType]chan *ProtoUserApps)
	Stats() *DeviceTypeSaverStats
}

type memcSavers struct {
	memcClients  map[DeviceType]*memcache.Client
	errCh        chan errs.Error
	done         chan struct{}
	wg           sync.WaitGroup
	dry          bool
	timeout      time.Duration
	maxRetries   int
	retryTimeout time.Duration
	stats        DeviceTypeSaverStats
}

// todo: as option - to improve performance 1) add workers per deviceType or/and 2) use memcache connections pool (if client has one ...)
func NewMemcSaver(ctx context.Context, addrs map[string]string, dry bool, timeout time.Duration, maxRetries int, retryTimeout time.Duration, statsOn bool) (Saver, errs.Error) {
	ctx = cou.BuildContext(ctx, cou.SetContextOperation("4.0.memc_saver_init"))
	memcClients := make(map[DeviceType]*memcache.Client, len(addrs))
	stats := DeviceTypeSaverStats{DTStats: make(map[DeviceType]*SaverStats, len(addrs)), StartTime: time.Now()}
	for dt, addr := range addrs {
		deviceType := DeviceType(dt)
		mc := memcache.New(addr)
		if mc == nil {
			return nil, errs.E(ctx, errs.SeverityCritical, errs.KindMemcache, fmt.Errorf("memcache client [%s] failed on addr [%s]", deviceType, addr))
		}
		mc.Timeout = timeout
		if err := mc.Ping(); err != nil {
			return nil, errs.E(ctx, errs.SeverityCritical, errs.KindMemcache, fmt.Errorf("memcache client [%s] is down on addr [%s]: %w", deviceType, addr, err))
		}
		// mc.MaxIdleConns
		memcClients[deviceType] = mc
		stats.DTStats[deviceType] = &SaverStats{
			ItemsCounter: regs.NewCounter(0, statsOn),
		}
		logging.Msg(ctx).Debugf("memc client [%s] on addr [%s] set with timeout = [%v] retries = [%d] retry_timeout = [%v]", deviceType, addr, timeout, maxRetries, retryTimeout)
	}
	errCh := make(chan errs.Error, len(addrs)*8)
	return &memcSavers{
		memcClients:  memcClients,
		errCh:        errCh,
		done:         make(chan struct{}),
		dry:          dry,
		timeout:      timeout,
		maxRetries:   maxRetries,
		retryTimeout: retryTimeout,
		stats:        stats,
	}, nil
}

func (r *memcSavers) Run(ctx context.Context, dtInputs map[DeviceType]chan *ProtoUserApps) {
	ctx = cou.BuildContext(ctx, cou.SetContextOperation("3.saving"))

	go func() {
		ctx = cou.BuildContext(ctx, cou.AddContextOperation("workers"))
		r.wg.Add(len(r.memcClients))
		// on exit
		defer func() {
			r.wg.Wait()
			r.stats.FinishTime = time.Now()
			for deviceType, memcClient := range r.memcClients {
				if err := memcClient.FlushAll(); err != nil {
					r.errCh <- errs.E(
						ctx,
						errs.SeverityCritical,
						errs.KindMemcache,
						fmt.Errorf("memcache client [%s] failed to flush: %w", deviceType, err),
					)
				}
			}
			close(r.errCh)
			close(r.done)
			logging.Msg(ctx).Debug("saver stopped:", ctx.Err())
		}()
		for deviceType := range r.memcClients {

			go func(ctx context.Context, deviceType DeviceType) {
				ctx = cou.BuildContext(ctx, cou.AddContextOperation(cou.Operation(fmt.Sprintf("mc-%s", deviceType))))
				logging.Msg(ctx).Debugf("memc [%s] - started", deviceType)
				defer OnExit(ctx, r.errCh, fmt.Sprintf("memc [%s]", deviceType), true,
					func() {
						r.wg.Done()
					})
				mc := r.memcClients[deviceType]
				inputs := dtInputs[deviceType]
				sts := r.stats.DTStats[deviceType]
				for {
					//select {
					//case <-ctx.Done():
					//	return
					//case protoUserApps, more := <-inputs:
					// !writer should close channel if ctx.Done()
					protoUserApps, more := <-inputs
					if !more {
						logging.Msg(ctx).Debugf("memc input channel [%s] - closed", deviceType)
						return
					}
					if !r.dry {
						delay := r.retryTimeout
						var err error
						for attempt := 0; attempt < r.maxRetries; attempt++ {
							if err = mc.Set(&memcache.Item{Key: protoUserApps.DeviceKey, Value: protoUserApps.Data}); err == nil {
								break
							}
							r.errCh <- errs.E(
								ctx,
								errs.SeverityWarning,
								errs.KindMemcache,
								fmt.Errorf(
									"memcache client [%s] failed to save item with key[%s]: %w",
									deviceType,
									protoUserApps.DeviceKey,
									err,
								),
							)
							delay = 2 * delay
							time.Sleep(delay)
						}
						if err != nil {
							r.errCh <- errs.E(
								ctx,
								errs.SeverityCritical,
								errs.KindMemcache,
								fmt.Errorf(
									"memcache client [%s] failed to save item with key[%s]: %w",
									deviceType,
									protoUserApps.DeviceKey,
									err,
								),
							)
							// todo: to avoid data loss, it needs to return item back to buffer somehow... or save it elsewhere (e.g. into log))
						} else {
							sts.ItemsCounter.Add(protoUserApps.Size())
						}
					} else {
						sts.ItemsCounter.Add(protoUserApps.Size())
					}
				}
			}(ctx, deviceType)
		}
	}()
}

func (r *memcSavers) ErrCh() <-chan errs.Error {
	return r.errCh
}

func (r *memcSavers) Done() <-chan struct{} {
	return r.done
}

func (r *memcSavers) Stats() *DeviceTypeSaverStats {
	return &r.stats
}

type DTSaverStats map[DeviceType]*SaverStats

func (m DTSaverStats) SortByDeviceType() []DeviceType {
	keys := make([]DeviceType, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}
