package workflow

import (
	"context"
	"fmt"
	"github.com/nj-eka/MemcLoadGo/appsinstalled"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/regs"
	"google.golang.org/protobuf/proto"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DeviceType string

type ProtoUserApps struct {
	DeviceKey string
	Data      []byte
}

func (d ProtoUserApps) Size() int {
	return len(d.DeviceKey) + len(d.Data)
}

type DeviceRecord struct {
	DeviceType          string
	ProtoDeviceUserApps ProtoUserApps
}

type ParserStats interface {
	ItemsCounter() regs.Counter
	InputBytesCounter() regs.Counter
	OutputBytesCounter() regs.Counter
}

type DeviceTypeParserStats struct {
	DTStats            map[DeviceType]ParserStats
	StartTime, EndTime time.Time
}

type parserStats struct {
	itemsCounter, inputBytesCounter, outputBytesCounter regs.Counter
}

func (r *parserStats) ItemsCounter() regs.Counter {
	return r.itemsCounter
}

func (r *parserStats) InputBytesCounter() regs.Counter {
	return r.inputBytesCounter
}

func (r *parserStats) OutputBytesCounter() regs.Counter {
	return r.outputBytesCounter
}

type Parser interface {
	Run(ctx context.Context) <-chan struct{}
	ResChs() map[DeviceType]chan *ProtoUserApps // <-chan *DeviceRecord
	ErrCh() <-chan errs.Error
	Stats() *DeviceTypeParserStats
}

type parser struct {
	resChs                  map[DeviceType]chan *ProtoUserApps
	errCh                   chan errs.Error
	stats                   DeviceTypeParserStats

	inputCh <-chan string
	maxWorkers              int
	ignoreUnknownDeviceType bool
}

func NewParser(ctx context.Context, inputCh <-chan string, maxWorkers int, deviceTypes map[DeviceType]bool, ignoreUnknownDeviceType bool, statsOn bool) Parser {
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("2.0.parser_init"))
	resChs := make(map[DeviceType]chan *ProtoUserApps, len(deviceTypes))
	dtStats := DeviceTypeParserStats{DTStats: make(map[DeviceType]ParserStats, len(deviceTypes))}
	for deviceType := range deviceTypes {
		resChs[deviceType] = make(chan *ProtoUserApps, maxWorkers)
		dtStats.DTStats[deviceType] = &parserStats{
			itemsCounter:       regs.NewCounter(0, statsOn),
			inputBytesCounter:  regs.NewCounter(0, statsOn),
			outputBytesCounter: regs.NewCounter(0, statsOn),
		}
	}
	return &parser{
		inputCh: inputCh,
		maxWorkers:              maxWorkers,
		resChs:                  resChs,
		errCh:                   make(chan errs.Error, maxWorkers * 4),
		ignoreUnknownDeviceType: ignoreUnknownDeviceType,
		stats:                   dtStats,
	}
}

func (r *parser) Run(ctx context.Context) <-chan struct{} {
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("2.parser_run"))
	r.stats.StartTime = time.Now()
	done := make(chan struct{})

	go func() {
		ctx = cu.BuildContext(ctx, cu.AddContextOperation("workers"))
		wg :=                      sync.WaitGroup{}
		wp := make(chan struct{}, r.maxWorkers)


		defer OnExit(ctx, r.errCh, "workers", true,
			func() {
				wg.Wait()
				r.stats.EndTime = time.Now()
				close(wp)
				for deviceType, resCh := range r.resChs {
					close(resCh)
					logging.Msg(ctx).Debug("parser channel [%s] - closed", deviceType)
				}
				close(r.errCh)
				close(done)
			})
		// ctx.Done() is ignored here for not to lose data written to inputCh. exit after input channel is closed and all received tasks/lines done/processed
		for line := range r.inputCh {
			wp <- struct{}{}
			wg.Add(1)
			go processInputString(ctx, &wg, wp, line, r.resChs, r.errCh, r.ignoreUnknownDeviceType, r.stats)
		}
	}()

	return done
}

func processInputString(ctx context.Context, wg *sync.WaitGroup, wp <-chan struct{}, inputString string, resChs map[DeviceType]chan *ProtoUserApps, errCh chan<- errs.Error, ignoreUnknownDeviceType bool, sts DeviceTypeParserStats) {
	ctx = cu.BuildContext(ctx, cu.AddContextOperation("parsing"))
	defer OnExit(ctx, errCh, "", false,
		func() {
			<-wp
			wg.Done()
		})
	parts := strings.Split(strings.TrimSpace(inputString), "\t")
	if len(parts) != 5 {
		errCh <- errs.E(ctx, errs.KindInvalidValue, fmt.Errorf("invalid record format: %s", inputString))
		return
	}

	deviceType, deviceId := DeviceType(parts[0]), parts[1]
	if _, exists := resChs[deviceType]; !exists && !ignoreUnknownDeviceType {
		errCh <- errs.E(ctx, errs.KindInvalidValue, fmt.Errorf("Unknown device type: %s", deviceType))
		return
	}
	//Apps []uint32 `protobuf:"varint,1,rep,name=apps" json:"apps,omitempty"`
	//Lat  *float64 `protobuf:"fixed64,2,opt,name=lat" json:"lat,omitempty"`
	//Lon  *float64 `protobuf:"fixed64,3,opt,name=lon" json:"lon,omitempty"`
	lat, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		errCh <- errs.E(ctx, errs.KindInvalidValue, fmt.Errorf("invalid lat: %s", parts[2]))
		return
	}
	lon, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		errCh <- errs.E(ctx, errs.KindInvalidValue, fmt.Errorf("invalid lon: %s", parts[3]))
		return
	}
	var apps []uint32
	for _, app := range strings.Split(parts[4], ",") {
		app = strings.TrimSpace(app)
		appId, err := strconv.ParseUint(app, 10, 32)
		if err != nil {
			errCh <- errs.E(ctx, errs.SeverityWarning, errs.KindInvalidValue, fmt.Errorf("app %s is not uint32", app))
			continue
		}
		apps = append(apps, uint32(appId))
	}
	deviceKey := fmt.Sprintf("%s:%s", deviceType, deviceId)
	userApps := &appsinstalled.UserApps{
		Apps: apps,
		Lat:  &lat,
		Lon:  &lon,
	}
	data, err := proto.Marshal(userApps)
	if err != nil {
		errCh <- errs.E(ctx, errs.KindProto, fmt.Errorf("proto marshaling [%s] failed ", inputString))
		return
	}

	protoUserApps := ProtoUserApps{
		DeviceKey: deviceKey,
		Data:      data,
	}
	resChs[deviceType] <- &protoUserApps

	sts.DTStats[deviceType].ItemsCounter().Add(1)
	sts.DTStats[deviceType].InputBytesCounter().Add(len(inputString))
	sts.DTStats[deviceType].OutputBytesCounter().Add(protoUserApps.Size())
}

type DTParserStats map[DeviceType]ParserStats

func (m DTParserStats) SortByDeviceType() []DeviceType {
	keys := make([]DeviceType, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func (r *parser) ResChs() map[DeviceType]chan *ProtoUserApps {
	return r.resChs
}

func (r *parser) ErrCh() <-chan errs.Error {
	return r.errCh
}

func (r *parser) Stats() *DeviceTypeParserStats {
	return &r.stats
}
