package errflow

import (
	"context"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/logging"

	"sync"
)

// MergeErrors merges multiple channels of errors.
// Based on https://blog.golang.org/pipelines.
func MergeErrors(ctx context.Context, errChs ...<-chan errs.Error) <-chan errs.Error {
	ctx = cu.BuildContext(ctx, cu.AddContextOperation("merging"))
	var wg sync.WaitGroup
	var capOut int
	for _, errCh := range errChs {
		if errCh != nil{
			capOut += cap(errCh)
		}
	}
	outputErrCh := make(chan errs.Error, capOut)

	output := func(errCh <-chan errs.Error) {
		for err := range errCh {
			outputErrCh <- err
		}
		wg.Done()
	}
	wg.Add(len(errChs))
	for _, errCh := range errChs {
		if errCh != nil {
			go output(errCh)
		}
	}

	go func() {
		wg.Wait()
		close(outputErrCh)
		logging.Msg(ctx).Debug("Merged errors channel - closed")
	}()
	return outputErrCh
}
