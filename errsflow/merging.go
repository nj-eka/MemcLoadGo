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
func MergeErrors(ctx context.Context, errorProducers ...ErrorProducer) <-chan errs.Error {
	ctx = cu.BuildContext(ctx, cu.AddContextOperation("a.merge"))
	var wg sync.WaitGroup
	// We must ensure that the output channel has the reading capacity to hold as many errors
	// as there could be written to all error channels at once.
	// This will ensure that it never blocks, even
	// if further processing ended before closing the channel.
	var capOut int
	for _, errorProducer := range errorProducers {
		capOut += cap(errorProducer.ErrCh())
	}
	cout := make(chan errs.Error, capOut)
	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(cerr <-chan errs.Error) {
		for err := range cerr {
			cout <- err
		}
		wg.Done()
	}
	wg.Add(len(errorProducers))
	for _, errorProducer := range errorProducers {
		go output(errorProducer.ErrCh())
	}
	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(cout)
		logging.Msg(ctx).Info("errs merged channel - closed")
	}()
	return cout
}
