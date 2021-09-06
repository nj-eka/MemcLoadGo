package regs

import (
	"sync"
)

func NewCounter(initValue int, on bool) Counter {
	return &counter{score: initValue, isOn: on}
}

type counter struct {
	sync.RWMutex
	count int
	score int
	isOn  bool
}

func (r *counter) Add(num int) int {
	if r.isOn {
		r.Lock()
		defer r.Unlock()
		r.count++
		r.score += num
	}
	return r.score
}

func (r *counter) GetCount() int {
	if r.isOn {
		r.RLock()
		defer r.RUnlock()
	}
	return r.count
}

func (r *counter) GetScore() int {
	if r.isOn {
		r.RLock()
		defer r.RUnlock()
	}
	return r.score
}

func (r *counter) GetCountScore() (count int, score int) {
	if r.isOn {
		r.RLock()
		defer r.RUnlock()
	}
	return r.count, r.score
}
