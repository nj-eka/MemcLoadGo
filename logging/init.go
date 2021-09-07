package logging

import (
	"context"
	"fmt"
	cou "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/fh"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"os/user"
	"runtime/trace"
	"strings"
)

const DEFAULT_LEVEL = logrus.InfoLevel
const DEFAULT_FORMAT = "text"
const DEFAULT_TIME_FORMAT = "2006/01/02 15:04:05.00000"

var logFile, traceFile *os.File

func Initialize(ctx context.Context, logFileName, level, format, traceFileName string, usr *user.User) errs.Error {
	ctx = cou.BuildContext(ctx, cou.AddContextOperation("log_init"))
	logrus.SetOutput(os.Stdout)
	logrus.RegisterExitHandler(Finalize)
	if logFileName != "" {
		var file *os.File
		var err error
		if logFileName, err := fh.ResolvePath(logFileName, usr); err != nil {
			return errs.E(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("invalid log file name <%s>: %w", logFileName, err))
		}
		file, err = os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
		if err != nil {
			return errs.E(ctx, errs.SeverityCritical, errs.KindOpenFile, fmt.Errorf("open file <%s> for logging failed: %w", logFileName, err))
		} else {
			logrus.SetOutput(file)
			logFile = file
		}
	}
	fieldMap := logrus.FieldMap{
		logrus.FieldKeyTime:  "ts",
		logrus.FieldKeyLevel: "lvl",
		logrus.FieldKeyMsg:   "msg"}
	switch strings.ToUpper(format) {
	case "JSON":
		logrus.SetFormatter(
			&ContextFormatter{
				&logrus.JSONFormatter{
					FieldMap: fieldMap,
				},
			})
	case "TEXT":
		logrus.SetFormatter(
			&ContextFormatter{
				&logrus.TextFormatter{
					ForceQuote:       false,
					DisableTimestamp: false,
					FullTimestamp:    true,
					TimestampFormat:  DEFAULT_TIME_FORMAT,
					QuoteEmptyFields: true,
					FieldMap:         fieldMap,
				},
			})
	default:
		return errs.E(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("invalid log format [%s]. supported formats: json, text", format))
	}
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return errs.E(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("parsing log level from config failed: %w", err))
	}
	logrus.SetLevel(lvl)
	Msg(ctx).Infof("Logging initialized with level <%s>", level)
	switch lvl {
	case logrus.DebugLevel:
		logrus.SetReportCaller(true)
	case logrus.TraceLevel:
		logrus.SetReportCaller(true)
		var err error
		if traceFileName == "" {
			traceFile = os.Stderr
		} else {
			if traceFileName, err := fh.ResolvePath(traceFileName, usr); err != nil {
				return errs.E(ctx, errs.SeverityCritical, errs.KindInvalidValue, fmt.Errorf("invalid trace file name <%s>: %w", traceFileName, err))
			}
			if traceFile, err = os.OpenFile(traceFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664); err != nil {
				return errs.E(ctx, errs.SeverityCritical, errs.KindOpenFile, fmt.Errorf("creating trace file failed: %w", err))
			}
		}
		if err := trace.Start(traceFile); err != nil {
			return errs.E(ctx, errs.SeverityCritical, errs.KindInternal, fmt.Errorf("starting trace failed: %w", err))
		}
	}
	errs.WithFrames(lvl > logrus.InfoLevel)
	log.SetOutput(logrus.StandardLogger().Writer()) // to use with standard log pkg
	if traceFile != nil {
		Msg(ctx).Tracef("Tracing started with output to %s", traceFile.Name())
	}
	return nil
}

func Finalize() {
	op := cou.Operation("log_finalize")
	if traceFile != nil {
		trace.Stop()
		Msg(op).Trace("trace stopped")
		if traceFile != os.Stderr {
			if err := traceFile.Close(); err != nil {
				Msg(op).Errorf("closing trace file failed: %v", err)
			} else {
				Msg(op).Trace("trace file closed")
			}
		}
	}
	if nil != logFile {
		Msg(op).Info("Logging finalized.")
		if err := logFile.Sync(); err != nil {
			Msg(op).Errorf("sync log buffer with file [%s] - failed: %v", logFile.Name(), err)
		}
		if err := logFile.Close(); err != nil {
			Msg(op).Errorf("closing log file [%s] - failed: %v\n", logFile.Name(), err)
		}
	}
}
