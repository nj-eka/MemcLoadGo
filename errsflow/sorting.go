package errflow

import (
	"context"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/reg"
)

func SortFilteredErrors(ctx context.Context, inputErrCh <-chan errs.Error, filterSeverities []errs.Severity, statsOn bool) (map[errs.Severity]chan errs.Error, ErrorStats) {
	ctx = cu.BuildContext(ctx, cu.AddContextOperation("sorting"))
	scerr := make(map[errs.Severity]chan errs.Error)
	stats := ErrorStats(reg.NewEncounter(len(errs.AllSeverities) * int(errs.KindInternal) * 64, statsOn))
	for _, severity := range filterSeverities {
		scerr[severity] = make(chan errs.Error, cap(inputErrCh))
	}
	go func() {
		defer func() {
			for severity, cerr := range scerr {
				close(cerr)
				logging.Msg(ctx).Debug("Error channel - ", severity.String(), " - closed")
			}
		}()
		for err := range inputErrCh {
			if err != nil {
				if cerr, ok := scerr[err.Severity()]; ok {
					cerr <- err
				}
				stats.CheckIn(ErrStatKey{err.Severity(), err.Kind(), err.OperationPath().String()})
			}
		}
	}()
	return scerr, stats
}
