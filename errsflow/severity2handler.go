package errflow

import (
	"context"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"

	"sync"
)

type FuncErrorHandler func(cerr <-chan errs.Error, wg *sync.WaitGroup)

func MapErrorHandlers(
	ctx context.Context,
	scerr map[errs.Severity]chan errs.Error,
	handlers map[errs.Severity]FuncErrorHandler,
	defaulthandler FuncErrorHandler,
) <-chan struct{} {
	logging.Msg(ctx).Debug("Errors handlers - start")
	done := make(chan struct{})
	var wg sync.WaitGroup
	for severity, cerr := range scerr {
		handler := defaulthandler
		if handlers != nil {
			if _, ok := handlers[severity]; ok {
				handler = handlers[severity]
			}
		}
		wg.Add(1)
		go handler(cerr, &wg)
	}
	go func() {
		wg.Wait()
		close(done)
		logging.Msg(ctx).Debug("Errors handlers - stop")
	}()
	return done
}
