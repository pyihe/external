package alias

import (
	"container/list"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type eventType uint8

const (
	none      eventType = 0
	effective eventType = 1
)

type Sampler interface {
	Pick() (bool, int)             // 进行一次采样
	PickN(int) (bool, map[int]int) // 进行N次采样
	Add(...Event) error            // 添加概率事件
	Remove(Event) error            // 删除概率事件
}

type Event interface {
	// Id 概率事件ID
	Id() int

	// Prob 概率事件发生的概率
	Prob() float64
}

type element struct {
	id   int
	prob float64
	typ  eventType
}

type core struct {
	mu    sync.RWMutex // guard blow
	evts  *queue       // 原始的概率事件
	prob  []*element   //
	alias []*element   //

	rad *rand.Rand
}

func New() Sampler {
	return &core{
		evts: newQueue(50),
		rad:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (c *core) Add(events ...Event) error {
	if len(events) == 0 {
		return nil
	}

	if err := c.addEvent(events...); err != nil {
		return err
	}

	c.construction()

	return nil
}

func (c *core) Remove(event Event) error {
	return c.removeEvent(event)
}

func (c *core) PickN(n int) (bool, map[int]int) {
	c.mu.RLock()
	l := c.evts.length()
	if c.evts.prob < 1 {
		l += 1
	}

	var (
		targetLine *element
		randomIdx  int
		prob       float64
	)
	result := make(map[int]int)
	for i := 0; i < n; i++ {
		randomIdx = c.rad.Intn(l)
		prob = c.rad.Float64()
		if targetLine = c.prob[randomIdx]; prob > targetLine.prob {
			targetLine = c.alias[randomIdx]
		}
		if targetLine.typ == effective {
			result[targetLine.id] += 1
		}
	}
	c.mu.RUnlock()

	return len(result) > 0, result
}

func (c *core) Pick() (ok bool, id int) {
	c.mu.RLock()
	var n = c.evts.length()
	if c.evts.prob < 1 {
		n += 1
	}
	var targetLine *element       // 最终的概率事件所在的列
	var randomIdx = c.rad.Intn(n) // 落在哪个概率事件所在的列中
	var prob = c.rad.Float64()    // 访问该列中随机事件的概率

	// 根据随机的概率来决定最终落在哪个事件上
	if targetLine = c.prob[randomIdx]; prob > targetLine.prob {
		targetLine = c.alias[randomIdx]
	}
	c.mu.RUnlock()

	if targetLine.typ == effective {
		id = targetLine.id
		ok = true
	}

	return
}

func (c *core) addEvent(event ...Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, e := range event {
		if e == nil {
			return errors.New("nil event")
		}
		if e.Prob() < 0 || e.Prob() > 1 {
			return fmt.Errorf("invalid probability, event id: %v", e.Id())
		}
		if sum := c.evts.prob + e.Prob(); sum > 1 || sum <= 0 {
			return errors.New("invalid total probability")
		}
		if exist, _, _ := c.evts.find(e.Id()); exist {
			return errors.New("repeated event id")
		}

		c.evts.add(e)
	}

	return nil
}

func (c *core) removeEvent(event Event) error {
	c.mu.Lock()
	c.evts.remove(event)
	c.mu.Unlock()

	c.construction()
	return nil
}

func (c *core) construction() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var small = list.New()
	var large = list.New()
	var n = c.evts.length()
	var totalProb = c.evts.prob

	if totalProb < 1 {
		n += 1
		e := &element{
			id:   -1,
			prob: truncFloat((1 - totalProb) * float64(n)),
			typ:  none,
		}
		if e.prob < 1 {
			small.PushFront(e)
		} else {
			large.PushFront(e)
		}
	}

	for _, event := range c.evts.data {
		e := &element{
			id:   event.Id(),
			prob: truncFloat(event.Prob() * float64(n)),
			typ:  effective,
		}
		if e.prob < 1 {
			small.PushFront(e)
		} else {
			large.PushFront(e)
		}
	}

	c.prob = make([]*element, 0, n)
	c.alias = make([]*element, 0, n)

	for small.Len() > 0 && large.Len() > 0 {
		l := small.Front()
		g := large.Front()
		small.Remove(l)
		large.Remove(g)

		le := l.Value.(*element)
		ge := g.Value.(*element)

		c.prob = append(c.prob, le)
		c.alias = append(c.alias, ge)

		ge.prob = truncFloat(ge.prob + le.prob - 1)
		if ge.prob < 1 {
			small.PushFront(ge)
		} else {
			large.PushFront(ge)
		}
	}

	for large.Len() > 0 {
		g := large.Front()
		ge := g.Value.(*element)
		ge.prob = 1
		c.prob = append(c.prob, ge)
		large.Remove(g)
	}
	for small.Len() > 0 {
		l := small.Front()
		le := l.Value.(*element)
		le.prob = 1
		c.prob = append(c.prob, le)
		small.Remove(l)
	}
}

func truncFloat(v float64) float64 {
	v, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", v), 64)
	return v
}
