package reserve

import "sync"

type binSubscriber struct {
	mtx  sync.Mutex
	subs map[uint8][]chan struct{}
}

func newBinSubscriber() *binSubscriber {
	return &binSubscriber{
		subs: make(map[uint8][]chan struct{}),
	}
}

func (b *binSubscriber) Subscribe(bin uint8) (<-chan struct{}, func()) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	c := make(chan struct{}, 1)
	b.subs[bin] = append(b.subs[bin], c)

	return c, func() {
		b.mtx.Lock()
		defer b.mtx.Unlock()

		for i, s := range b.subs[bin] {
			if s == c {
				b.subs[bin] = append(b.subs[bin][:i], b.subs[bin][i+1:]...)
				break
			}
		}
	}
}

func (b *binSubscriber) Trigger(bin uint8) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	for _, s := range b.subs[bin] {
		select {
		case s <- struct{}{}:
		default:
		}
	}
}
