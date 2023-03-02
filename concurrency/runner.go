package concurrency

import (
	"github.com/pyihe/external/internal"
)

type runner struct {
	list *taskList
}

func (w *runner) reset() {
	w.list = nil
}

func (w *runner) run() {
	tList := w.list
	tList.waiter.Add(1)
	go func() {
		for {
			var t *task

			// 从任务链表中pop一个任务
			t = tList.pop()

			// 如果任务链表空了，则需要停止当前runner，并释放
			if t == nil {
				tList.addRunnerNum(-1)
				tList.pool.release(w)
				tList.waiter.Done()
				return
			}

			// 同步执行任务
			internal.ExecWithRecover(t.f)
			// 任务执行完毕后需要回收
			tList.pool.release(t)
		}
	}()
}
