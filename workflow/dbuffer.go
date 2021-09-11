package workflow

import (
	"context"
	"fmt"
	"github.com/joncrlsn/dque"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/reg"
	"sort"
	"sync"
	"time"
)

type DeviceTypeDBuffer interface {
	Run(context.Context) <-chan struct{}
	ResChs() map[DeviceType]chan *ProtoUserApps
	ErrCh() <-chan errs.Error
	Stats() interface{}
}

type dbuf struct {
	que       *dque.DQue
	wg        sync.WaitGroup
	inputDone chan struct{}
}

func (r *dbuf) Enqueue(item *ProtoUserApps) error {
	r.wg.Add(1)
	if err := r.que.Enqueue(item); err == nil {
		return nil
	} else {
		r.wg.Done()
		return err
	}
}

func (r *dbuf) Dequeue() (*ProtoUserApps, error) {
	if data, err := r.que.Dequeue(); err == nil {
		defer r.wg.Done()
		if result, ok := data.(*ProtoUserApps); ok {
			return result, nil
		} else {
			return nil, fmt.Errorf("dque internal error")
		}
	} else {
		return nil, err
	}
}

type deviceTypeDBuffer struct {
	errCh        chan errs.Error
	stats        *DeviceTypeDBufferStats

	dtInputChs map[DeviceType]chan *ProtoUserApps
	dtOutputChs  map[DeviceType]chan *ProtoUserApps

	workersCount int
	dBufs        map[string]*dbuf
}

func NewDTDBuffer(ctx context.Context, dtInputChs map[DeviceType]chan *ProtoUserApps, workersCount int, dirPath string, itemsPerSegment int, resume bool, turbo bool, statsOn bool) (DeviceTypeDBuffer, error) {
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("3.0.dbuf_init"))
	outputChs := make(map[DeviceType]chan *ProtoUserApps, len(dtInputChs))
	dBufs := make(map[string]*dbuf, len(dtInputChs)*workersCount)
	stats := DeviceTypeDBufferStats{
		DTInputStats:  make(map[string]*DBufferStats, len(dBufs)),
		DTOutputStats: make(map[string]*DBufferStats, len(dBufs)),
	}
	for deviceType, inputCh := range dtInputChs {
		for workerNum := 0; workerNum < workersCount; workerNum++ {
			workerName := GetWorkerName(deviceType, workerNum)
			open := dque.New
			if resume {
				open = dque.NewOrOpen
			}
			if que, err := open(workerName, dirPath, itemsPerSegment, ProtoUserAppsBuilder); err == nil {
				dBuf := dbuf{
					que:       que,
					inputDone: make(chan struct{}),
				}
				if turbo {
					_ = que.TurboOn()
				}
				logging.Msg(ctx).Debugf("openning dbuf dque: %s/%s [%d] with turbo - %s resume - %s", dirPath, deviceType, itemsPerSegment, turbo, resume)
				logging.Msg(ctx).Debugf("dbuf dque [%s/%s] open with size: %d", dirPath, deviceType, que.Size())
				if que.Size() > 0 {
					dBuf.wg.Add(que.Size())
				}
				dBufs[workerName] = &dBuf
				stats.DTInputStats[workerName] = &DBufferStats{
					ItemsCounter: reg.NewCounter(0, statsOn),
				}
				stats.DTOutputStats[workerName] = &DBufferStats{
					ItemsCounter: reg.NewCounter(0, statsOn),
				}
			} else {
				return nil, errs.E(ctx, errs.SeverityCritical, errs.KindDBuff, fmt.Errorf("init dbuffer [%s] failed: %w ", deviceType, err))
			}
		}
		outputChs[deviceType] = make(chan *ProtoUserApps, cap(inputCh))
	}

	return &deviceTypeDBuffer{
		dtInputChs: dtInputChs,
		dtOutputChs:  outputChs,
		dBufs:        dBufs,
		workersCount: workersCount,
		errCh:        make(chan errs.Error, len(dtInputChs)*workersCount*8),
		stats:        &stats,
	}, nil
}

func (r *deviceTypeDBuffer) Run(ctx context.Context) <-chan struct{}  {
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("3.dbuf"))
	done := make(chan struct{})

	go func(){
		ctx = cu.BuildContext(ctx, cu.AddContextOperation("workers"))
		var wg sync.WaitGroup
		wg.Add(2 * len(r.dtInputChs) * r.workersCount)
		r.stats.StartTime = time.Now()
		defer OnExit(ctx, r.errCh, "workers", func(){
			wg.Wait()
			for _, outputCh := range r.dtOutputChs{
				close(outputCh)
			}
			close(r.errCh)
			close(done)
		})

		// -> buffer
		for deviceType, inputCh := range r.dtInputChs {
			for workerNum := 0; workerNum < r.workersCount; workerNum++ {
				workerName := GetWorkerName(deviceType, workerNum)

				go func(ctx context.Context, wg *sync.WaitGroup, workerName string, inputCh <-chan *ProtoUserApps, dBuf *dbuf, stats *DBufferStats) {
					ctx = cu.BuildContext(ctx, cu.AddContextOperation(cu.Operation(fmt.Sprintf("enqueue %s", workerName))))
					logging.Msg(ctx).Debugf("start pumping into [%s]", workerName)
					defer func() {
						stats.EndTime = time.Now()
						close(dBuf.inputDone)
						wg.Done()
						logging.Msg(ctx).Debugf("stop pumping into [%s]: %d / %d", workerName, stats.ItemsCounter.GetCount(), stats.ItemsCounter.GetScore())
					}()
					for protoUserApps := range inputCh {
						if err := dBuf.Enqueue(protoUserApps); err == nil {
							stats.ItemsCounter.Add(protoUserApps.Size())
						} else {
							// todo: add err handler for error with {errs.SeverityCritical, errs.KindDBuff} than calls cancel() on ctx to stop processing (data loss!)
							r.errCh <- errs.E(ctx, errs.SeverityCritical, errs.KindDBuff, fmt.Errorf("enqueue to dbuf [%s] failed on item [%v] with err: %w", workerName, protoUserApps, err))
						}
						//runtime.Gosched() // todo: maybe not so often ...
					}
				}(ctx, &wg, workerName, inputCh, r.dBufs[workerName], r.stats.DTInputStats[workerName])
			}
		}

		// buffer ->
		for deviceType := range r.dtInputChs {
			for workerNum := 0; workerNum < r.workersCount; workerNum++ {
				workerName := GetWorkerName(deviceType, workerNum)

				go func(ctx context.Context, wg *sync.WaitGroup, workerName string, outputCh chan<- *ProtoUserApps, dBuf *dbuf, stats *DBufferStats) {
					ctx = cu.BuildContext(ctx, cu.AddContextOperation(cu.Operation(fmt.Sprintf("dequeue %s", workerName))))
					logging.Msg(ctx).Debugf("start pumping out from [%s]", workerName)
					defer OnExit(ctx, r.errCh, fmt.Sprintf("pumping out from [%s]", workerName), func() {
						stats.EndTime = time.Now()
						if dBuf.que.Turbo() {
							err := dBuf.que.TurboSync()
							logging.Msg(ctx).Debugf("dbuf que [%s] turbo sync - done: %v", workerName, err)
						}
						err := dBuf.que.Close()
						logging.Msg(ctx).Debugf("dbuf que [%s] - closed: %v", workerName, err)
						wg.Done()
						logging.Msg(ctx).Debugf("stop pumping out from [%s]: %d(%d)", workerName, stats.ItemsCounter.GetCount(), stats.ItemsCounter.GetScore())
					})
					for {
						select {
						case <-ctx.Done():
							return
						default:
						}
						if item, err := dBuf.Dequeue(); err == nil {
							// below it is assumed that receiver is reading everything from channel,
							// otherwise todo: add timeout and return received record back to buffer
							outputCh <- item
							stats.ItemsCounter.Add(item.Size())
							// runtime.Gosched()
						} else {
							switch err {
							case dque.ErrQueueClosed:
								return
							case dque.ErrEmpty:
								select {
								case <-dBuf.inputDone:
									return
								default:
								}
								time.Sleep(10 * time.Millisecond) // todo: add const to config
								continue
							default:
								r.errCh <- errs.E(ctx, errs.KindDBuff, fmt.Errorf("dequeue to dbuf [%s] failed: %w", deviceType, err))
								// todo: try to enqueue dequeued item back ...
								return
							}
						}
					}
				}(ctx, &wg, workerName, r.dtOutputChs[deviceType], r.dBufs[workerName], r.stats.DTOutputStats[workerName])
			}
		}
	}()

	return done
}

func (r *deviceTypeDBuffer) ResChs() map[DeviceType]chan *ProtoUserApps {
	return r.dtOutputChs
}

func (r *deviceTypeDBuffer) ErrCh() <-chan errs.Error {
	return r.errCh
}

func (r *deviceTypeDBuffer) Stats() interface{} {
	return r.stats
}

func GetWorkerName(deviceType DeviceType, workerNum int) string {
	return fmt.Sprint(deviceType, "_", workerNum)
}

func ProtoUserAppsBuilder() interface{} {
	return &ProtoUserApps{}
}

type DBufferStats struct {
	ItemsCounter reg.Counter
	EndTime      time.Time
}

type DeviceTypeDBufferStats struct {
	StartTime     time.Time
	DTInputStats  map[string]*DBufferStats
	DTOutputStats map[string]*DBufferStats
}

type DTBufferStats map[string]*DBufferStats

func (m DTBufferStats) SortByDeviceType() []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}
