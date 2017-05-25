package gogorabbit

type queues map[string]*queue

func (q queues) AddQueue(queue *queue) {
	q[queue.Name] = queue
}

func (q queues) GetQueue(name string) (queue *queue, ok bool) {
	queue, ok = q[name]

	return
}

type queue struct {
	options
	consumers
	Name         string
	ExchangeName string
	BindKey      string
}
