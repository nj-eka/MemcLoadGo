package workflow

import (
	"context"
)

type Runner interface{
	Run(context.Context) <-chan struct{}
}

func Run(ctx context.Context, runners ... Runner) <-chan struct{} {
	doneChs := make([]<-chan struct{}, 0, len(runners))
	for _, runner := range runners{
		doneChs = append(doneChs, runner.Run(ctx))
	}
	done := make(chan struct{})
	go func() {
		for _, dc := range doneChs{
			<-dc
		}
		close(done)
	}()
	return done
}
