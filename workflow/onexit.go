package workflow

import (
	"context"
	"fmt"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
)

func OnExit(ctx context.Context, cerr chan<- errs.Error, prefixMsg string, withMsg bool, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("%v", r)
			}
			logging.LogError(errs.E(ctx, errs.KindInternal, errs.SeverityCritical, fmt.Errorf("%s - failed: error channel closed + %w", prefixMsg, err)))
		}
	}()
	if r := recover(); r != nil {
		err, ok := r.(error)
		if !ok {
			err = fmt.Errorf("%v", r)
		}
		cerr <- errs.E(ctx, errs.KindInternal, errs.SeverityCritical, fmt.Errorf("%s - interrupted: %w", prefixMsg, err)) // panic on writing to closed channel
	} else {
		select {
		case <-ctx.Done():
			cerr <- errs.E(ctx, errs.KindInterrupted, fmt.Errorf("%s - interrupted: %w", prefixMsg, ctx.Err())) // panic on writing to closed channel
		default:
			fn()
			if withMsg {
				logging.Msg(ctx).Debug(prefixMsg, " - ok")
			}
		}
	}
}
