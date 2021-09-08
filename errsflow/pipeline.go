package errflow

import (
	"context"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/regs"
)

type ErrorsStat struct {
	Done  <-chan struct{}
	Stats regs.Decounter
}

func LaunchErrorHandlers(ctx context.Context, cancel context.CancelFunc, statsOn bool, errsChs ...<-chan errs.Error) (<-chan struct{}, regs.Decounter) {
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("_.errs_pipe"))
	errsCh := MergeErrors(ctx, errsChs...)
	mscerrs, errsStats := SortFilteredErrors(ctx, errsCh, logging.GetSeveritiesFilter4CurrentLogLevel(), statsOn)
	errsDone := MapErrorHandlers(
		ctx,
		mscerrs,
		map[errs.Severity]FuncErrorHandler{
			errs.SeverityCritical: CriticalErrorHandlerBuilder(cancel, []errs.Kind{errs.KindDBuff}),
		},
		LoggingErrorHandler,
	)
	return errsDone, errsStats
}
