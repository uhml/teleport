package backend

import (
	"bytes"
	"context"
	"sync"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

type CircularBuffer struct {
	sync.Mutex
	*log.Entry
	ctx      context.Context
	cancel   context.CancelFunc
	events   []Event
	start    int
	end      int
	size     int
	watchers []*BufferWatcher
}

func NewCircularBuffer(ctx context.Context, size int) (*CircularBuffer, error) {
	if size <= 0 {
		return nil, trace.BadParameter("bad circular buffer")
	}
	ctx, cancel := context.WithCancel(ctx)
	buf := &CircularBuffer{
		Entry: log.WithFields(log.Fields{
			trace.Component: "buffer",
		}),
		ctx:    ctx,
		cancel: cancel,
		events: make([]Event, size),
		start:  -1,
		end:    -1,
		size:   0,
	}
	return buf, nil
}

func (c *CircularBuffer) Close() error {
	c.cancel()
	c.Lock()
	defer c.Unlock()
	for _, w := range c.watchers {
		w.Close()
	}
	c.watchers = nil
	return nil
}

func (c *CircularBuffer) Size() int {
	return c.size
}

// Events returns a copy of records as arranged from start to end
func (c *CircularBuffer) Events() []Event {
	c.Lock()
	defer c.Unlock()
	events, _ := c.eventsCopy()
	return events
}

// eventsCopy returns a copy of events as arranged from start to end
func (c *CircularBuffer) eventsCopy() ([]Event, error) {
	if c.size == 0 {
		return nil, nil
	}
	var out []Event
	for i := 0; i < c.size; i++ {
		index := (c.start + i) % len(c.events)
		if out == nil {
			out = make([]Event, 0, c.size)
		}
		out = append(out, c.events[index])
	}
	return out, nil
}

// PushBatch pushes elements to the queue as a batch
func (c *CircularBuffer) PushBatch(events []Event) {
	c.Lock()
	defer c.Unlock()

	for i := range events {
		c.push(events[i])
	}
}

// Push pushes elements to the queue
func (c *CircularBuffer) Push(r Event) {
	c.Lock()
	defer c.Unlock()
	c.push(r)
}

func (c *CircularBuffer) push(r Event) {
	if c.size == 0 {
		c.start = 0
		c.end = 0
		c.size = 1
	} else if c.size < len(c.events) {
		c.end = (c.end + 1) % len(c.events)
		c.events[c.end] = r
		c.size += 1
	} else {
		c.end = c.start
		c.start = (c.start + 1) % len(c.events)
	}
	c.events[c.end] = r
	c.fanOutEvent(r)
}

func matchPrefix(prefix []byte, e Event) bool {
	if prefix == nil {
		return true
	}
	return bytes.HasPrefix(e.Item.Key, prefix)
}

func (c *CircularBuffer) fanOutEvent(r Event) {
	for i, watcher := range c.watchers {
		if !matchPrefix(watcher.prefix, r) {
			continue
		}
		select {
		case watcher.eventsC <- r:
		case <-c.ctx.Done():
			return
		default:
			c.Warningf("Closing watcher, buffer overflow.")
			watcher.Close()
			c.watchers = append(c.watchers[:i], c.watchers[i+1:]...)
		}
	}
}

func (c *CircularBuffer) NewWatcher(ctx context.Context, watch Watch) (Watcher, error) {
	c.Lock()
	defer c.Unlock()

	select {
	case <-c.ctx.Done():
		return nil, trace.BadParameter("buffer is closed")
	default:
	}

	closeCtx, cancel := context.WithCancel(ctx)
	w := &BufferWatcher{
		prefix:  watch.Prefix,
		eventsC: make(chan Event, len(c.events)),
		ctx:     closeCtx,
		cancel:  cancel,
	}
	c.watchers = append(c.watchers, w)
	return w, nil
}

func max(a, b int) int {
	if a > b {
		return b
	}
	return a
}

type BufferWatcher struct {
	prefix  []byte
	eventsC chan Event
	ctx     context.Context
	cancel  context.CancelFunc
}

func (w *BufferWatcher) Events() <-chan Event {
	return w.eventsC
}

func (w *BufferWatcher) Done() <-chan struct{} {
	return w.ctx.Done()
}

func (w *BufferWatcher) Close() error {
	w.cancel()
	return nil
}
