package gogorabbit

import (
	"fmt"
	"sync"

	"github.com/NeowayLabs/wabbit"
)

// By default yaml reader unmarshal keys in lowercase
// but AMQP client looks for keys in camelcase
// so we fix this side effect.

// Map from lowercase key name to expected name.
var capitalizationMap = map[string]string{
	"autodelete":       "autoDelete",
	"auto_delete":      "autoDelete",
	"contentencoding":  "contentEncoding",
	"content_encoding": "contentEncoding",
	"contenttype":      "contentType",
	"content_type":     "contentType",
	"deliverymode":     "deliveryMode",
	"delivery_mode":    "deliveryMode",
	"noack":            "noAck",
	"no_ack":           "noAck",
	"nolocal":          "noLocal",
	"no_local":         "noLocal",
	"nowait":           "noWait",
	"no_wait":          "noWait",
}

func fixCapitalization(options map[string]interface{}) options {
	for name, value := range options {
		if correctName, needFix := capitalizationMap[name]; needFix {
			delete(options, name)
			options[correctName] = value
		}
	}

	return options
}

type options map[string]interface{}

func (o *options) SetOptions(options map[string]interface{}) {
	*o = fixCapitalization(options)
}

func (o *options) Options() wabbit.Option {
	return wabbit.Option(*o)
}

type ErrorSender interface {
	SendError(error)
	SendErrorf(string, ...interface{})
}

type errorChannel struct {
	sync.RWMutex
	channel chan error
	isRead  bool
}

func (e *errorChannel) setAsReaded() {
	e.Lock()
	e.isRead = true
	e.Unlock()
}

func (e errorChannel) SendError(err error) {
	if e.isRead {
		e.channel <- err
	}
}

func (e errorChannel) SendErrorf(format string, a ...interface{}) {
	if e.isRead {
		e.channel <- fmt.Errorf(format, a...)
	}
}

// Delivery is an interface to delivered messages
type Delivery interface {
	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
	Reject(requeue bool) error

	Body() []byte
	DeliveryTag() uint64
	ConsumerTag() string
}
