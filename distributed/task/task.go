package etcdtask

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pyihe/external/concurrency"
	"github.com/pyihe/external/cronexpr"
	"github.com/pyihe/external/internal"
	"github.com/pyihe/external/map"
)

const (
	statusNormal   = 1 // 任务状态：正常
	statusDel      = 2 // 任务状态：删除
	execLockPrefix = "task/exec_locker"
	concurrencyKey = "etcdtask"
)

var (
	ErrClosedTaskCron   = errors.New("operate on closed task cron")
	ErrRepeatedRegister = errors.New("repeated register")
	ErrInvalidCronExpr  = errors.New("invalid cron expr")
)

type (
	Handler func(key string, value interface{})

	task struct {
		Key     string      `json:"key,omitempty"`      // 任务key
		EtcdKey string      `json:"etcd_key,omitempty"` // 任务etcd key
		Value   interface{} `json:"value,omitempty"`    // 任务value
		Spec    string      `json:"spec,omitempty"`     // spec
		Status  uint8       `json:"status,omitempty"`   // 任务状态
		Timeout int         `json:"timeout,omitempty"`  // 任务执行超时时间
		Handler Handler     `json:"-"`                  // 任务handler
	}

	TaskCron struct {
		prefix       string // 任务前缀
		client       *clientv3.Client
		tasks        *gomap.LockMap[string, *task]
		taskPool     sync.Pool
		cronExprPool sync.Pool
		status       int32
		cancelFunc   context.CancelFunc
	}
)

func New(client *clientv3.Client, prefix string) *TaskCron {
	ctx, cancel := context.WithCancel(context.Background())

	tc := &TaskCron{}
	tc.cancelFunc = cancel
	tc.status = internal.StatusInitial
	tc.client = client
	tc.prefix = prefix
	tc.tasks = gomap.NewLockMap[string, *task]()
	tc.taskPool.New = func() any {
		return &task{}
	}
	tc.cronExprPool.New = func() any {
		return &cronexpr.Expr{}
	}
	concurrency.Go(concurrencyKey, func() {
		tc.watch(ctx)
	})
	return tc
}

func (tc *TaskCron) Close() {
	if !atomic.CompareAndSwapInt32(&tc.status, internal.StatusRunning, internal.StatusClosed) {
		return
	}
	tc.cancelFunc()
	concurrency.Stop(concurrencyKey)
}

func (tc *TaskCron) Add(spec, key, value string, taskTimeout int, handler Handler) (err error) {
	if tc.isClosed() {
		err = ErrClosedTaskCron
		return
	}

	etcdKey := tc.buildEtcdKey(key, value)
	oldTask, err := tc.getTask(etcdKey)
	if err != nil {
		return err
	}
	if oldTask != nil {
		return ErrRepeatedRegister
	}
	if taskTimeout <= 0 {
		taskTimeout = -1
	}

	schedule, err := tc.createCronExpr(spec)
	if err != nil {
		return err
	} else {
		defer tc.release(schedule)
	}

	now := time.Now()
	nextTime := schedule.Next(now)
	if nextTime.IsZero() {
		err = ErrInvalidCronExpr
		return
	}

	t := tc.createTask()
	t.Key = key
	t.Value = value
	t.EtcdKey = etcdKey
	t.Status = statusNormal
	t.Timeout = taskTimeout
	t.Handler = handler
	t.Spec = spec

	vBytes, err := json.Marshal(t)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rsp, err := tc.client.Grant(ctx, nextTime.Unix()-now.Unix())
	if err != nil {
		return
	}

	_, err = tc.client.Put(ctx, t.EtcdKey, string(vBytes), clientv3.WithLease(rsp.ID))
	if err == nil {
		tc.tasks.Set(etcdKey, t)
	} else {
		tc.release(t)
	}
	return
}

func (tc *TaskCron) Remove(key, value string) error {
	if tc.isClosed() {
		return ErrClosedTaskCron
	}
	etcdKey := tc.buildEtcdKey(key, value)
	t, err := tc.getTask(etcdKey)
	if err != nil {
		return err
	}
	if t == nil {
		return nil
	} else {
		defer tc.release(t)
	}

	t.Status = statusDel

	vBytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err = tc.client.Put(ctx, t.EtcdKey, string(vBytes)); err != nil {
		return err
	}

	_, err = tc.client.Delete(ctx, t.EtcdKey)
	if err == nil {
		tc.tasks.Del(etcdKey)
	}
	return err
}

func (tc *TaskCron) UpdateTime(key, value, spec string) error {
	if tc.isClosed() {
		return ErrClosedTaskCron
	}
	schedule, err := tc.createCronExpr(spec)
	if err != nil {
		return err
	}

	now := time.Now()
	nextTime := schedule.Next(now)
	if nextTime.IsZero() {
		return ErrInvalidCronExpr
	}

	etcdKey := tc.buildEtcdKey(key, value)
	t, err := tc.getTask(etcdKey)
	if err != nil {
		return err
	}

	if t == nil {
		return nil
	}

	t.Spec = spec
	vBytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rsp, err := tc.client.Grant(ctx, nextTime.Unix()-now.Unix())
	if err != nil {
		return err
	}

	_, err = tc.client.Put(ctx, etcdKey, string(vBytes), clientv3.WithLease(rsp.ID))
	if err == nil {
		tc.tasks.Set(etcdKey, t)
	}
	return err
}

func (tc *TaskCron) isClosed() bool {
	return atomic.LoadInt32(&tc.status) == internal.StatusClosed
}

func (tc *TaskCron) watch(ctx context.Context) {
	tc.status = internal.StatusRunning
	watchChan := tc.client.Watch(ctx, tc.prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())

	for events := range watchChan {
		if err := events.Err(); err != nil {
			continue
		}

		for i := range events.Events {

			evt := events.Events[i]
			if evt.Type != mvccpb.DELETE {
				continue
			}

			var (
				del   bool
				t     = tc.createTask()
				value = evt.PrevKv.Value
			)

			if len(value) == 0 {
				continue
			}
			if err := json.Unmarshal(value, &t); err != nil || t.EtcdKey == "" {
				continue
			}

			if t.Status == statusDel {
				continue
			}

			now := time.Now()
			schedule, _ := tc.createCronExpr(t.Spec)
			nextTime := schedule.Next(now)
			tc.release(schedule)
			if !nextTime.IsZero() { // 需要再次执行
				rsp, err := tc.client.Grant(context.Background(), nextTime.Unix()-now.Unix())
				if err != nil {
					continue
				}
				_, err = tc.client.Put(context.Background(), t.EtcdKey, string(value), clientv3.WithLease(rsp.ID))
				if err != nil {
					continue
				}
			} else {
				// 只执行一次的话，那么本次执行完毕后需要删除
				del = true
			}
			concurrency.Go(concurrencyKey, func() {
				defer func() {
					if del {
						tc.tasks.Del(t.EtcdKey)
					}
					tc.release(t)
				}()
				tc.runTask(t)
			})
		}
	}
}

func (tc *TaskCron) runTask(t *task) {
	var (
		ctx = context.Background()
		ttl = int64(t.Timeout)
	)
	if ttl <= 0 {
		ttl = math.MaxInt32
	}

	rsp, err := tc.client.Grant(ctx, int64(t.Timeout))
	if err != nil {
		return
	}

	defer tc.client.Revoke(ctx, rsp.ID)

	execKey := path.Join(execLockPrefix, t.EtcdKey)
	txnRsp, err := tc.client.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(execKey), "=", 0)).
		Then(clientv3.OpPut(execKey, "", clientv3.WithLease(rsp.ID))).Commit()
	if err != nil {
		return
	}

	// 没有抢到
	if !txnRsp.Succeeded {
		return
	}

	mTask, _ := tc.tasks.Get(t.EtcdKey)
	if mTask == nil || mTask.Handler == nil {
		return
	}

	timer := time.NewTimer(time.Duration(ttl) * time.Second)
	finish := make(chan struct{})
	concurrency.Go(concurrencyKey, func() {
		mTask.Handler(mTask.Key, mTask.Value)
		finish <- struct{}{}
	})
	select {
	case <-timer.C:
	case <-finish:
	}
	timer.Stop()
}

func (tc *TaskCron) buildEtcdKey(key, value string) string {
	return path.Join(tc.prefix, key, value)
}

func (tc *TaskCron) getTask(etcdKey string) (*task, error) {
	// 内存中是否存在
	t, exist := tc.tasks.Get(etcdKey)
	if exist && t != nil {
		return t, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rsp, err := tc.client.Get(ctx, etcdKey)
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) > 0 {
		t = tc.createTask()
		err = json.Unmarshal(rsp.Kvs[0].Value, &t)
	}
	return t, err
}

func (tc *TaskCron) createTask() *task {
	return tc.taskPool.Get().(*task)
}

func (tc *TaskCron) createCronExpr(spec string) (expr *cronexpr.Expr, err error) {
	expr = tc.cronExprPool.Get().(*cronexpr.Expr)
	err = cronexpr.ParseTo(spec, expr)
	if err != nil {
		tc.release(expr)
	}
	return
}

func (tc *TaskCron) release(v interface{}) {
	switch v.(type) {
	case *task:
		t := v.(*task)
		*t = task{}
		tc.taskPool.Put(t)
	case *cronexpr.Expr:
		expr := v.(*cronexpr.Expr)
		expr.Reset()
		tc.cronExprPool.Put(expr)
	}
}
