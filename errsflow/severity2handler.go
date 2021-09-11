package errflow

import (
	"context"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"sync"
)

type FuncErrorHandler func(cerr <-chan errs.Error, wg *sync.WaitGroup)

func MapErrorHandlers(
	ctx context.Context,
	scerr map[errs.Severity]chan errs.Error,
	handlers map[errs.Severity]FuncErrorHandler,
	defaultHandler FuncErrorHandler,
) <-chan struct{} {
	ctx = cu.BuildContext(ctx, cu.AddContextOperation("handling"))
	done := make(chan struct{})
	var wg sync.WaitGroup
	for severity, cerr := range scerr {
		handler := defaultHandler
		if handlers != nil {
			if _, ok := handlers[severity]; ok {
				handler = handlers[severity]
			}
		}
		wg.Add(1)
		go handler(cerr, &wg)
		logging.Msg(ctx).Debug("Errors handlers for [", severity, "] - started")
	}
	go func() {
		wg.Wait()
		close(done)
		logging.Msg(ctx).Debug("Errors handlers - stopped")
	}()
	return done
}
