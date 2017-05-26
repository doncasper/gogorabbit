package gogorabbit

type queues map[string]*queue

func (queues queues) AddQueue(queue *queue) {
	queues[queue.name] = queue
}

func (queues queues) GetQueue(name string) (queue *queue, ok bool) {
	queue, ok = queues[name]

	return
}

type queue struct {
	options
	consumers
	name         string
	exchangeName string
	bindKey      string
}
