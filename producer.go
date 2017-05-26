package gogorabbit

import (
	"sync"

	"github.com/NeowayLabs/wabbit"
)

type producers map[string]*producer

func (producers producers) AddProducer(producer *producer) {
	producers[producer.name] = producer
}

func (producers producers) GetProducer(name string) (producer *producer, ok bool) {
	producer, ok = producers[name]

	return
}

type producer struct {
	options
	sync.RWMutex   // Protect channel during reconnect.
	name           string
	exchangeName   string
	routingKey     string
	channel        wabbit.Channel
	publishChannel chan []byte
}

// Method safely sets new RMQ channel.
func (producer *producer) setChannel(channel wabbit.Channel) {
	producer.Lock()
	producer.channel = channel
	producer.Unlock()
}

func (producer *producer) Produce(data []byte) {
	producer.publishChannel <- data
}

func (producer *producer) worker() {
	for message := range producer.publishChannel {
		if err := producer.produce(message); err != nil {
			// TODO: Return errors to errorChannel for logging!
		}
	}
}

func (producer *producer) produce(message []byte) (err error) {
	producer.RLock()
	err = producer.channel.Publish(producer.exchangeName, producer.routingKey, message, producer.Options())
	producer.RUnlock()

	return
}
