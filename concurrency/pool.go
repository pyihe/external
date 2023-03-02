package concurrency

import (
	"sync"
	"sync/atomic"

	gomap "github.com/pyihe/external/map"
)

const (
	defaultRunnerCap = 1000
	defaultThreshold = 1
)

type Pool interface {
	// Stop 将会等待所有的协程执行完毕才返回，如果不需要等待协程执行完毕，不用调用此方法
	Stop(string)

	// SetThreshold 设置需要开启新的执行者的阈值
	SetThreshold(n int32)

	// SetRunnerCap 设置执行者的最大容量
	SetRunnerCap(n int32)

	// Go 添加一个任务
	Go(key string, f func())
}

// task goroutine执行的最小单元
type task struct {
	f    func() // handler
	next *task  // 下一个task
}

func (t *task) reset() {
	t.f = nil
	t.next = nil
}

type taskList struct {
	key       string
	runnerNum int32

	// guard below
	mu      sync.Mutex
	taskNum int32
	head    *task
	tail    *task

	waiter sync.WaitGroup
	pool   *pool
}

func (tl *taskList) push(t *task) {
	if t == nil {
		return
	}

	tl.mu.Lock()
	if tl.head == nil {
		tl.head = t
		tl.tail = t
	} else {
		tl.tail.next = t
		tl.tail = t
	}
	tl.mu.Unlock()
	tl.addTaskNum(1)
}

func (tl *taskList) pop() (t *task) {
	tl.mu.Lock()
	if tl.head != nil {
		t = tl.head
		tl.head = tl.head.next
	}
	tl.mu.Unlock()
	if t != nil {
		tl.addTaskNum(-1)
	}
	return
}

func (tl *taskList) addTaskNum(n int32) {
	atomic.AddInt32(&tl.taskNum, n)
}

func (tl *taskList) loadTaskNum() int32 {
	return atomic.LoadInt32(&tl.taskNum)
}

func (tl *taskList) addRunnerNum(n int32) {
	atomic.AddInt32(&tl.runnerNum, n)
}

func (tl *taskList) loadRunnerNum() int32 {
	return atomic.LoadInt32(&tl.runnerNum)
}

type pool struct {
	threshold int32
	runnerCap int32

	listPool   *gomap.LockMap[string, *taskList]
	taskPool   sync.Pool
	runnerPool sync.Pool
}

func NewPool(threshold, runnerCap int32) Pool {
	if threshold <= 0 {
		threshold = defaultThreshold
	}
	if runnerCap <= 0 {
		runnerCap = defaultRunnerCap
	}
	return &pool{
		threshold: threshold,
		runnerCap: runnerCap,
		listPool:  gomap.NewLockMap[string, *taskList](),
		taskPool: sync.Pool{New: func() any {
			return &task{}
		}},
		runnerPool: sync.Pool{New: func() any {
			return &runner{}
		}},
	}
}

func (p *pool) SetThreshold(n int32) {
	if n <= 0 {
		n = defaultThreshold
	}
	atomic.StoreInt32(&p.threshold, n)
}

func (p *pool) SetRunnerCap(n int32) {
	if n <= 0 {
		n = defaultRunnerCap
	}
	atomic.StoreInt32(&p.runnerCap, n)
}

func (p *pool) Go(key string, f func()) {
	l, ok := p.listPool.Get(key)
	if !ok {
		l = &taskList{
			key:  key,
			pool: p,
		}
	}

	l.push(p.newTask(f))

	if (l.loadTaskNum() >= atomic.LoadInt32(&p.threshold) && l.loadRunnerNum() < atomic.LoadInt32(&p.runnerCap)) || l.loadRunnerNum() == 0 {
		l.addRunnerNum(1)
		r := p.newRunner(l)
		r.run()
	}
}

func (p *pool) Stop(key string) {
	switch {
	case key == "":
		p.listPool.SafeRange(func(k string, v *taskList) bool {
			v.waiter.Wait()
			return true
		})
	default:
		list, ok := p.listPool.Get(key)
		if ok {
			list.waiter.Wait()
		}
	}
}

func (p *pool) newTask(f func()) *task {
	t := p.taskPool.Get().(*task)
	t.f = f
	return t
}

func (p *pool) newRunner(l *taskList) *runner {
	r := p.runnerPool.Get().(*runner)
	r.list = l
	return r
}

func (p *pool) release(v interface{}) {
	switch v.(type) {
	case *task:
		t := v.(*task)
		t.reset()
		p.taskPool.Put(t)
	case *runner:
		r := v.(*runner)
		r.reset()
		p.runnerPool.Put(r)

	case *taskList:
		list := v.(*taskList)
		p.listPool.Set(list.key, list)
	}
}
