package regs

import (
	"sync"
)

func NewDecounter(initCap int, on bool) Decounter {
	return &decounter{hits: make(map[interface{}]Counter, initCap), isOn: on}
}

type decounter struct {
	sync.RWMutex
	hits map[interface{}]Counter
	isOn bool
}

func (r *decounter) GetCounterPairs() []CounterPair {
	if !r.isOn {
		return []CounterPair{}
	}
	r.RLock()
	defer r.RUnlock()
	pairs := make([]CounterPair, 0, len(r.hits))
	// Note: counter values may change during iteration (due to concurrency),
	// if not applicable, use Encounter, it has "read commited" isolation level
	for k, c := range r.hits {
		pairs = append(pairs, CounterPair{k, c.GetScore()}) //
	}
	return pairs
}

func (r *decounter) CheckIn(key interface{}) int {
	if !r.isOn {
		return 0
	}
	r.RLock()
	if c, ok := r.hits[key]; ok {
		r.RUnlock()
		return c.Add(1)
	}
	r.RUnlock()
	// ...
	r.Lock()
	defer r.Unlock()
	if _, ok := r.hits[key]; !ok {
		r.hits[key] = NewCounter(0, false)
	}
	return r.hits[key].Add(1)
}

func (r *decounter) GetScores() map[interface{}]int {
	if !r.isOn {
		return make(map[interface{}]int)
	}
	r.RLock()
	defer r.RUnlock()
	result := make(map[interface{}]int, len(r.hits))
	for k, c := range r.hits {
		result[k] = c.GetScore()
	}
	return result
}

func (r *decounter) KeysCount() int {
	if !r.isOn {
		return 0
	}
	r.RLock()
	defer r.RUnlock()
	return len(r.hits)
}

func (r *decounter) TotalCount() (totalCount int) {
	if !r.isOn {
		return 0
	}
	r.RLock()
	defer r.RUnlock()
	for _, count := range r.hits {
		totalCount += count.GetScore()
	}
	return
}
