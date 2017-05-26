package gogorabbit

import (
	"fmt"

	"github.com/NeowayLabs/wabbit"
)

type ConsumerHandler func(delivery Delivery, sender ErrorSender)

type consumerWorkers []*consumerWorker

func (w *consumerWorkers) AddWorker(worker *consumerWorker) {
	*w = append(*w, worker)
}

type consumerWorker struct {
	errorChannel
	tag          string
	done         chan struct{}
}

func (w *consumerWorker) Handle(deliveries <-chan wabbit.Delivery, handler ConsumerHandler) {
	for delivery := range deliveries {
		handler(delivery, w.errorChannel)
	}

	w.SendError(fmt.Errorf("Consumer worker is stoped: %s", w.tag))
	w.done <- struct{}{}
}

type consumers map[string]*consumer

func (c consumers) AddConsumer(consumer *consumer) {
	c[consumer.name] = consumer
}

func (c consumers) GetConsumer(name string) (consumer *consumer, ok bool) {
	consumer, ok = c[name]

	return
}

type consumer struct {
	options
	consumerWorkers
	errorChannel
	channel      wabbit.Channel
	name         string
	workersCount int
	queueName    string
	exchangeName string
	handler      ConsumerHandler
}

func (c *consumer) CreateWorkers() {
	for i := 1; i <= c.workersCount; i++ {
		worker := &consumerWorker{
			tag:          fmt.Sprintf("%s_%d", c.name, i),
			done:         make(chan struct{}, 1),
			errorChannel: c.errorChannel,
		}

		c.AddWorker(worker)
	}
}

func (c *consumer) RunWorkers() (err error) {
	for _, worker := range c.consumerWorkers {
		if err = c.runWorker(worker); err != nil {
			return
		}
	}

	return
}

func (c *consumer) setChannel(channel wabbit.Channel) error {
	c.channel = channel

	return c.channel.Qos(1, 0, false)
}

func (c *consumer) runWorker(worker *consumerWorker) (err error) {
	deliveries, err := c.channel.Consume(
		c.queueName,
		worker.tag,
		c.Options(),
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	go worker.Handle(deliveries, c.handler)

	return
}
