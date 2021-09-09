package errflow

import (
	"context"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/regs"
)

type ErrorStats regs.Decounter

type ErrorModerator interface{
	Run(ctx context.Context) <-chan struct{}
	Stats() interface {}
}

type errorModerator struct{
	done <- chan struct{}
	stats ErrorStats
}

func NewErrorModerator(ctx context.Context, cancel context.CancelFunc, statsOn bool, errsChs ...<-chan errs.Error) (ErrorModerator, errs.Error){
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("_.errs_init"))
	errsCh := MergeErrors(ctx, errsChs...)
	mapSeverity2ErrorCh, totalErrorStats := SortFilteredErrors(ctx, errsCh, logging.GetSeveritiesFilter4CurrentLogLevel(), statsOn)
	errsDone := MapErrorHandlers(
		ctx,
		mapSeverity2ErrorCh,
		map[errs.Severity]FuncErrorHandler{
			errs.SeverityCritical: CriticalErrorHandlerBuilder(cancel, []errs.Kind{errs.KindDBuff}),
		},
		LoggingErrorHandler,
	)
	return &errorModerator{
		done:  errsDone,
		stats: totalErrorStats,
	}, nil
}

func (r *errorModerator) Run(ctx context.Context) <-chan struct{} {
	return r.done
}

func (r *errorModerator) Stats() interface{} {
	return r.stats
}
