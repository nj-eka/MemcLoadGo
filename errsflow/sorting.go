package errflow

import (
	"context"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/regs"
)

// SortFilteredErrors
func SortFilteredErrors(ctx context.Context, cerr <-chan errs.Error, filterSeverities []errs.Severity, statsOn bool) (map[errs.Severity]chan errs.Error, regs.Decounter) {
	scerr := make(map[errs.Severity]chan errs.Error)
	stats := regs.NewDecounter(len(errs.AllSeverities)*int(errs.KindInternal)*64, statsOn)
	for _, severity := range filterSeverities {
		scerr[severity] = make(chan errs.Error, cap(cerr))
	}
	go func() {
		defer func() {
			for sev, cerr := range scerr {
				close(cerr)
				logging.Msg(ctx).Debug("ERRCh - ", sev.String(), " - closed")
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
