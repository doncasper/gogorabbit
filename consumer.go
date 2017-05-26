package gogorabbit

import (
	"fmt"

	"github.com/NeowayLabs/wabbit"
)

type consumerWorkers []*consumerWorker

func (workers *consumerWorkers) AddWorker(worker *consumerWorker) {
	*workers = append(*workers, worker)
}

type consumerWorker struct {
	tag    string
	done   chan struct{}
}

func (workers *consumerWorker) Handle(deliveries <-chan wabbit.Delivery, handler ConsumerHandler) {
	for d := range deliveries {
		if err := handler(d.Body()); err != nil {
			d.Nack(false, true)

			continue
		}

		d.Ack(false)
	}

	workers.done <- struct{}{}
}

type consumers map[string]*consumer

func (consumers consumers) AddConsumer(consumer *consumer) {
	consumers[consumer.name] = consumer
}

func (consumers consumers) GetConsumer(name string) (consumer *consumer, ok bool) {
	consumer, ok = consumers[name]

	return
}

type consumer struct {
	options
	consumerWorkers
	channel      wabbit.Channel
	name         string
	workersCount int
	queueName    string
	exchangeName string
	handler      ConsumerHandler
}

func (consumer *consumer) CreateWorkers() {
	for i := 1; i <= consumer.workersCount; i++ {
		worker := &consumerWorker{
			tag:  fmt.Sprintf("%s_%d", consumer.name, i),
			done: make(chan struct{}, 1),
		}

		consumer.AddWorker(worker)
	}
}

func (consumer *consumer) RunWorkers() (err error) {
	for _, worker := range consumer.consumerWorkers {
		if err = consumer.runWorker(worker); err != nil {
			return
		}
	}

	return
}

func (consumer *consumer) runWorker(worker *consumerWorker) (err error) {
	deliveries, err := consumer.channel.Consume(
		consumer.queueName,
		worker.tag,
		consumer.Options(),
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	go worker.Handle(deliveries, consumer.handler)

	return
}
