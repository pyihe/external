package gosync

import (
	"sync"

	"github.com/pyihe/external/concurrency"
)

const concurrencyKey = "waitGroup"

type WaitWrapper struct {
	sync.WaitGroup
}

func (w *WaitWrapper) Wrap(f func()) {
	w.Add(1)
	concurrency.Go(concurrencyKey, func() {
		f()
		w.Done()
	})
}
