package regs

type Counter interface {
	Add(num int) int
	GetCount() int
	GetScore() int
	GetCountScore() (count int, score int)
}

type Decounter interface {
	CheckIn(interface{}) int
	GetScores() map[interface{}]int
	GetCounterPairs() []CounterPair
	KeysCount() int
	TotalCount() int
}

type CounterPair struct {
	Key   interface{}
	Count int
}

type CounterPairsByKey []CounterPair

type Lesser interface {
	Less(interface{}) bool
}

func (c CounterPairsByKey) Len() int {
	return len(c)
}

func (c CounterPairsByKey) Less(i, j int) bool {
	if lesser, ok := c[i].Key.(Lesser); ok {
		return lesser.Less(c[j].Key)
	}
	panic("key type is not implement Lesser interface")
}

func (c CounterPairsByKey) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
