package workflow

type Pipeliner interface{
	Runner
//	errflow.ErrorProducer
	StatProducer
}

type Pipelines []Pipeliner

func (pl Pipelines) Runners() []Runner{
	result := make([]Runner, 0, len(pl))
	for _, p := range pl{
		result = append(result, p)
	}
	return result
}

func (pl Pipelines) StatProducers() []StatProducer{
	result := make([]StatProducer, 0, len(pl))
	for _, p := range pl{
		result = append(result, p)
	}
	return result
}
