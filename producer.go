package gogorabbit

import (
	"sync"

	"github.com/NeowayLabs/wabbit"
)

type producers map[string]*producer

func (p producers) AddProducer(producer *producer) {
	p[producer.name] = producer
}

func (p producers) GetProducer(name string) (producer *producer, ok bool) {
	producer, ok = p[name]

	return
}

type Producer interface {
	Produce([]byte)
}

type producer struct {
	options
	errorChannel
	sync.RWMutex   // Protect channel during reconnect.
	name           string
	exchangeName   string
	routingKey     string
	channel        wabbit.Channel
	publishChannel chan []byte
}

// Method safely sets new RMQ channel.
func (p *producer) setChannel(channel wabbit.Channel) {
	p.Lock()
	p.channel = channel
	p.Unlock()
}

func (p *producer) Name() string {
	return p.name
}

func (p *producer) Produce(data []byte) {
	p.publishChannel <- data
}

func (p *producer) worker() {
	for message := range p.publishChannel {
		if err := p.produce(message); err != nil {
			p.SendErrorf("Can't produce: %s", err)
		}
	}
}

func (p *producer) produce(message []byte) (err error) {
	p.RLock()
	err = p.channel.Publish(p.exchangeName, p.routingKey, message, p.Options())
	p.RUnlock()

	return
}
