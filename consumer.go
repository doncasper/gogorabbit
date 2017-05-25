package gogorabbit

import (
	"fmt"

	"github.com/NeowayLabs/wabbit"
	"github.com/Sirupsen/logrus"
)

type consumerWorkers []*consumerWorker

func (w *consumerWorkers) AddWorker(worker *consumerWorker) {
	*w = append(*w, worker)
}

type consumerWorker struct {
	Tag    string
	Done   chan struct{}
	logger logrus.FieldLogger
}

func (w *consumerWorker) Handle(deliveries <-chan wabbit.Delivery, handler ConsumerHandler) {
	for d := range deliveries {
		if err := handler(d.Body()); err != nil {
			w.logger.Errorf("Handler error for consumer %s: %v", d.ConsumerTag(), err)

			d.Nack(false, true)

			continue
		}

		d.Ack(false)
	}

	w.logger.Infof("Consumers worker: %s is stopped!", w.Tag)
	w.Done <- struct{}{}
}

type consumers map[string]*consumer

func (c consumers) AddConsumer(consumer *consumer) {
	c[consumer.Name] = consumer
}

func (c consumers) GetConsumer(name string) (consumer *consumer, ok bool) {
	consumer, ok = c[name]

	return
}

type consumer struct {
	options
	consumerWorkers
	channel      wabbit.Channel
	Name         string
	WorkersCount int
	QueueName    string
	ExchangeName string
	Handler      ConsumerHandler
	logger       logrus.FieldLogger
}

func (c *consumer) CreateWorkers() {
	for i := 1; i <= c.WorkersCount; i++ {
		worker := &consumerWorker{
			Tag:  fmt.Sprintf("%s_%d", c.Name, i),
			Done: make(chan struct{}, 1),
		}
		worker.logger = c.logger.WithField("logger", "consumer."+worker.Tag)

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

func (c *consumer) runWorker(worker *consumerWorker) (err error) {
	worker.logger.Debug("Running consumer worker!")

	deliveries, err := c.channel.Consume(
		c.QueueName,
		worker.Tag,
		c.Options(),
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	go worker.Handle(deliveries, c.Handler)

	return
}
