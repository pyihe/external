package alias

type queue struct {
	data []Event
	prob float64
}

func newQueue(size int) *queue {
	return &queue{data: make([]Event, 0, size)}
}

func (q *queue) length() int {
	return len(q.data)
}

func (q *queue) add(event Event) {
	n := len(q.data)
	c := cap(q.data)

	if n+1 > c {
		nq := make([]Event, n, c*2)
		copy(nq, q.data)
		q.data = nq
	}

	q.data = q.data[:n+1]
	q.data[n] = event
	q.prob = truncFloat(q.prob + event.Prob())
}

func (q *queue) remove(event Event) {
	exist, idx, _ := q.find(event.Id())
	if !exist {
		return
	}

	n, c := len(q.data), cap(q.data)
	if n < (c/2) && c > 25 {
		nq := make([]Event, n, c/2)
		copy(nq, q.data)
		q.data = nq
	}

	// 将idx后面的元素前移
	copy(q.data[idx:], q.data[idx+1:])
	// 最后将队列末尾元素删除
	q.data[n-1] = nil
	q.data = q.data[:n-1]
	q.prob = truncFloat(q.prob - event.Prob())
}

func (q *queue) find(eventId int) (exist bool, idx int, event Event) {
	for i, e := range q.data {
		if e.Id() == eventId {
			idx = i
			exist = true
			event = e
			break
		}
	}
	return
}
