package workflow

import "context"

type StatProducer interface{
	Stats() interface{}
}

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

type Pipeliner interface{
	Runner
	StatProducer
}

type Pipeline []Pipeliner

func (pl Pipeline) Runners() []Runner{
	result := make([]Runner, 0, len(pl))
	for _, p := range pl{
		result = append(result, p)
	}
	return result
}

func (pl Pipeline) StatProducers() []StatProducer{
	result := make([]StatProducer, 0, len(pl))
	for _, p := range pl{
		result = append(result, p)
	}
	return result
}
