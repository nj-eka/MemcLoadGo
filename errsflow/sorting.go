package errflow

import (
	"context"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/regs"
)

func SortFilteredErrors(ctx context.Context, cerr <-chan errs.Error, filterSeverities []errs.Severity, statsOn bool) (map[errs.Severity]chan errs.Error, ErrorStats) {
	ctx = cu.BuildContext(ctx, cu.AddContextOperation("b.sort"))
	scerr := make(map[errs.Severity]chan errs.Error)
	stats := ErrorStats(regs.NewDecounter(len(errs.AllSeverities)*int(errs.KindInternal)*64, statsOn))
	for _, severity := range filterSeverities {
		scerr[severity] = make(chan errs.Error, cap(cerr))
		logging.Msg(ctx).Debug("errs channel [", severity.String(), "] - opened")
	}
	go func() {
		defer func() {
			for severity, cerr := range scerr {
				close(cerr)
				logging.Msg(ctx).Debug("errs channel [", severity.String(), "] - closed")
			}
		}()
		for err := range cerr {
			if err != nil {
				stats.CheckIn(ErrStatKey{err.Severity(), err.Kind(), err.OperationPath().String()})
				if cerr, ok := scerr[err.Severity()]; ok {
					cerr <- err
				}
			}
		}
	}()
	return scerr, stats
}
