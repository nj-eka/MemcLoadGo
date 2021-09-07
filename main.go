package main

import (
	"context"
	"encoding/json"
	"fmt"
	conf "github.com/heetch/confita"
	"github.com/heetch/confita/backend/env"
	"github.com/heetch/confita/backend/file"
	"github.com/heetch/confita/backend/flags"
	cou "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	errflow "github.com/nj-eka/MemcLoadGo/errsflow"
	"github.com/nj-eka/MemcLoadGo/fh"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/output"
	"github.com/nj-eka/MemcLoadGo/workflow"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"time"
)

const (
	DefaultPattern                 = "./data/appsinstalled/*.tsv.gz"
	DefaultMemcTimeout             = 0
	DefaultDry                     = true
	DefaultIgnoreUnknownDeviceType = true
	DefaultVerbose                 = false
	DefaultMemcMaxRetries          = 3
	DefaultDQuesWorkersCount       = 2
	DefaultDquesResume             = true
	DefaultDquesTurbo              = false
	DefaultDquesSegmentSeize       = 64 * 1024
)

var (
	AppName           = filepath.Base(os.Args[0])
	DefaultConfigFile = "config.yml" // fmt.Sprintf("./%s.yml", AppName)
	DefaultLogFile    = fmt.Sprintf("%s.log", AppName)
	DefaultTraceFile  = fmt.Sprintf("%s.trace.out", AppName)
	DefaultDquesDir   = "data/dques/"
	DefaultMaxLoaders = 1 // runtime.NumCPU()
	DefaultMaxParsers = runtime.NumCPU() * 4
)

type MemcacheAddresses struct {
	IDFA string `config:"idfa,description=memc address for 'idfa' device type"`
	GAID string `config:"gaid,description=memc address for 'gaid' device type"`
	ADID string `config:"adid,description=memc address for 'adid' device type"`
	DVID string `config:"dvid,description=memc address for 'dvid' device type"`
}

type Config struct {
	//// 0. logging
	// logging output file, if empty then os.Stdout
	LogFile string `config:"log,description=Path to logging output file (empty = os.Stdout)" yaml:"log_file"`
	// logrus logging levels: panic, fatal, error, warn / warning, info, debug, trace
	LogLevel string `config:"log_level,short=l,description=Logging level: panic fatal error warn info debug trace" yaml:"log_level"`
	// supported logging formats: text, json
	LogFormat string `config:"log_format,description=Logging format: text json" yaml:"log_format"`
	//// 0.1 trace
	// Go execution tracer output file (tracing is on if LogLevel == trace)
	TraceFile string `config:"trace,description=Trace output file (tracing is on if LogLevel == trace)" yaml:"trace_file"`

	//// 1. app data
	//// 1.0 app mode
	// Execution mode without modification
	IsDry bool `config:"dry,description=Execution mode without modification" yaml:"is_dry"`
	// skip errors for unknown input device type
	IgnoreUnknownDeviceType bool `config:"unkignore,decription=Skip errors for unknown input device type" yaml:"ignore_unknown"`
	// 1.1 input
	// input files pattern. example: ./data/appsinstalled/h1000*.tsv.gz
	Pattern string `config:"pattern,short=p,description=Input files pattern" yaml:"pattern"`
	// 1.2 output
	//memc_addr:
	//  idfa: 127.0.0.1:33013
	//  gaid: 127.0.0.1:33014
	//  adid: 127.0.0.1:33015
	//  dvid: 127.0.0.1:33016
	MemcAddrs map[string]string `config:"memc_addrs" yaml:"memc_addrs,omitempty"`
	// confita doesn't support field type 'map' for flags and env backends
	// Workarounds:
	// 1. use other flags package with map fields support; e.g. "github.com/jessevdk/go-flags"
	// 2. use predefined sets - here
	// 3. extend confita...
	MemcAddrsPredefined MemcacheAddresses

	//// 2. workers
	//// 2.0
	// Display processing statistics (os.Stdout)
	Verbose bool `config:"verbose,short=v,description=Display processing statistics (os.Stdout)" yaml:"verbose"`
	//// 2.1
	// Max count of loaders (max number of open input files)
	MaxLoaders int `config:"loaders,description=Max count of loaders (max number of open input files)" yaml:"max_loaders_count"`
	// Max count of concurrent data parsing workers (input line -> proto data -> device channel)
	MaxParsers int `config:"parsers,description=Max count of parsers" yaml:"max_parsers_count"`
	//// 2.2 durable buffering
	DQuesDir          string `config:"dques,description=Dqueue directory" yaml:"dques_dir"`
	DQuesWorkersCount int    `config:"buffers,description=Buffers count per device type" yaml:"dques_buffers_count"`
	DQueResume        bool   `config:"resume,description=Resume from previous sessions" yaml:"dques_resume"`
	DQuesTurbo        bool   `config:"turbo,description=Dqueue turbo mode" yaml:"dques_turbo"`
	DQuesSegmentSize  int    `config:"segment,description=Items per dqueue segment" yaml:"dques_segment_size"`
	// 2.3 memcache
	// Timeout specifies the socket read/write timeout. If zero, DefaultTimeout is used.100 * time.Millisecond
	MemcTimeout time.Duration `config:"timeout,description=memcache operation timeout" yaml:"memc_timeout"`
	// maximum number of write attempts without generating error
	MemcMaxRetries int `config:"retries,description=memcache max retries" yaml:"memc_retries"`
	// min timeout between retries
	MemcRetryTimeout time.Duration `config:"retry_timeout,description=memcache retry timeout" yaml:"memc_retry_timeout"`

}

var cfg = Config{
	LogFile:                 DefaultLogFile,
	LogLevel:                logging.DefaultLevel.String(),
	LogFormat:               logging.DefaultFormat,
	TraceFile:               DefaultTraceFile,
	Pattern:                 DefaultPattern,
	MaxLoaders:              DefaultMaxLoaders,
	MaxParsers:              DefaultMaxParsers,
	IsDry:                   DefaultDry,
	IgnoreUnknownDeviceType: DefaultIgnoreUnknownDeviceType,
	Verbose:                 DefaultVerbose,
	MemcTimeout:             DefaultMemcTimeout,
	MemcMaxRetries:          DefaultMemcMaxRetries,
	MemcAddrs:               make(map[string]string),
	DQuesDir:                DefaultDquesDir,
	DQuesWorkersCount:       DefaultDQuesWorkersCount,
	DQueResume:              DefaultDquesResume,
	DQuesTurbo:              DefaultDquesTurbo,
	DQuesSegmentSize:        DefaultDquesSegmentSeize,
}

var (
	currentUser *user.User
	inputFiles  []string
	deviceTypes = make(map[workflow.DeviceType]bool)
	startTime   = time.Now()
)

func init() {
	ctx := cou.BuildContext(context.Background(), cou.SetContextOperation("00.init"))
	var err error
	loader := conf.NewLoader(
		file.NewBackend(DefaultConfigFile),
		env.NewBackend(),
		flags.NewBackend(),
	)
	if err = loader.Load(ctx, &cfg); err != nil {
		logging.LogError(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("invalid config: %w", err))
		log.Exit(1)
	}
	// memc_addrs predefined sets - current workaround
	if cfg.MemcAddrsPredefined.IDFA != "" {
		cfg.MemcAddrs["idfa"] = cfg.MemcAddrsPredefined.IDFA
	}
	if cfg.MemcAddrsPredefined.GAID != "" {
		cfg.MemcAddrs["gaid"] = cfg.MemcAddrsPredefined.GAID
	}
	if cfg.MemcAddrsPredefined.ADID != "" {
		cfg.MemcAddrs["adid"] = cfg.MemcAddrsPredefined.ADID
	}
	if cfg.MemcAddrsPredefined.DVID != "" {
		cfg.MemcAddrs["dvid"] = cfg.MemcAddrsPredefined.DVID
	}
	for deviceType := range cfg.MemcAddrs {
		deviceTypes[workflow.DeviceType(deviceType)] = true
	}
	if err = logging.Initialize(ctx, cfg.LogFile, cfg.LogLevel, cfg.LogFormat, cfg.TraceFile, currentUser); err != nil {
		logging.LogError(err)
		log.Exit(1)
	}
	if cfg.Pattern, err = fh.ResolvePath(cfg.Pattern, currentUser); err != nil {
		logging.LogError(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("invalid pattern: %w", err))
		log.Exit(1)
	}
	if inputFiles, err = filepath.Glob(cfg.Pattern); err != nil {
		logging.LogError(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("invalid pattern: %w", err))
		log.Exit(1)
	}
	if cfg.DQuesDir, err = fh.ResolvePath(cfg.DQuesDir, currentUser); err != nil {
		logging.LogError(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("invalid dbuffer dir [%s]: %w", cfg.DQuesDir, err))
		log.Exit(1)
	}
	if err = os.MkdirAll(cfg.DQuesDir, 0755); err != nil {
		logging.LogError(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("create dbuffer dir [%s] failed: %w", cfg.DQuesDir, err))
		log.Exit(1)
	}
	cfgJson, _ := json.Marshal(cfg)
	logging.Msg(ctx).Infof("%s started with pid %d", AppName, os.Getpid())
	logging.Msg(ctx).Debugf("options: %v", string(cfgJson))
}

var doneChs []<-chan struct{}

func allDone(ds []<-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for i := 0; i < len(ds); i++ {
			<-ds[i]
		}
		close(done)
	}()
	return done
}

func main() {
	defer logging.Finalize()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	ctx = cou.BuildContext(ctx, cou.SetContextOperation("0.main"))
	defer func() {
		<-allDone(append(doneChs, ctx.Done()))
	}()
	logging.Msg(ctx).Debug("start listening for signals")
	go func() {
		<-ctx.Done()
		cancel() // stop listening for signed signals asap
		logging.Msg(ctx).Debugf("stop listening for signed signals: %v", ctx.Err())
	}()
	defer cancel() // in case of early return (on error) - signal to close already running goroutines

	if len(inputFiles) == 0 {
		logging.LogError(ctx, errs.SeverityError, fmt.Errorf("no files found for pattern %s", cfg.Pattern))
		return
	}
	// workflow init
	loader := workflow.NewLoader(ctx, cfg.MaxLoaders, currentUser, cfg.IsDry, cfg.Verbose)
	parser := workflow.NewParser(ctx, cfg.MaxParsers, deviceTypes, cfg.IgnoreUnknownDeviceType, cfg.Verbose)
	dbuf, err := workflow.NewDTDBuffer(ctx, parser.ResChs(), cfg.DQuesWorkersCount, cfg.DQuesDir, cfg.DQuesSegmentSize, cfg.DQueResume, cfg.DQuesTurbo, cfg.Verbose)
	if err != nil {
		logging.LogError(err)
		return
	}
	saver, err := workflow.NewMemcSaver(ctx, cfg.MemcAddrs, cfg.IsDry, cfg.MemcTimeout, cfg.MemcMaxRetries, cfg.MemcRetryTimeout, cfg.Verbose)
	if err != nil {
		logging.LogError(err)
		return
	}
	// make error handling flow available
	errsMonitors := errflow.LaunchErrorHandlers(ctx, cancel, cfg.Verbose, loader.ErrCh(), parser.ErrCh(), dbuf.ErrCh(), saver.ErrCh())
	// compose done channels
	doneChs = append(doneChs, loader.Done(), parser.Done(), dbuf.Done(), saver.Done(), errsMonitors.Done)
	finish := allDone(doneChs)

	// start processing
	loader.Run(ctx, inputFiles)
	parser.Run(ctx, loader.ResCh())
	dbuf.Run(ctx, parser.ResChs())
	saver.Run(ctx, dbuf.ResChs())

mainloop:
	for {
		select {
		case <-ctx.Done():
			logging.Msg(ctx).Errorf("processing - interrupted: %v", ctx.Err())
			fmt.Println("stopping...\nwait for all processes to complete safely")
			break mainloop
		case <-finish:
			logging.Msg(ctx).Debug("processing - done")
			break mainloop
		case <-time.After(1 * time.Second):
			output.PrintProcessMonitors(startTime, cfg.Verbose, loader.Stats(), parser.Stats(), dbuf.Stats(), saver.Stats(), errsMonitors)
		}
	}
	<-finish
	output.PrintProcessMonitors(startTime, cfg.Verbose, loader.Stats(), parser.Stats(), dbuf.Stats(), saver.Stats(), errsMonitors)
}
