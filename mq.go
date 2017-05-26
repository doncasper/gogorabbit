package gogorabbit

import (
	"fmt"
	"sync"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
)

// After each failed attempt to establish a connection to the
// RabbitMQ, reconnect delay will be increased in percent.
const reconnectTimeStep = 15

type DoneChannel chan struct{}

type RabbitMQ struct {
	sync.Mutex
	exchanges
	errorChannel
	dsn            string
	reconnectDelay time.Duration // Seconds
	connection     wabbit.Conn
	channel        wabbit.Channel
}

type mqConfig map[string]interface{}

func (c mqConfig) GetInt(key string) int {
	return c[key].(int)
}

func (c mqConfig) GetString(key string) string {
	return c[key].(string)
}

func (c mqConfig) GetStringMap(key string) map[string]interface{} {
	return c[key].(map[string]interface{})
}

func New(dsn string, reconnectDelay time.Duration) (*RabbitMQ, error) {
	var err error

	rabbit := &RabbitMQ{
		exchanges:      make(exchanges),
		dsn:            dsn,
		reconnectDelay: reconnectDelay * time.Second,
		errorChannel:   make(chan error, 5),
	}

	err = rabbit.connect()
	if err != nil {
		return nil, fmt.Errorf("Error dialing RabbitMQ connection: %s", err)
	}

	rabbit.channel, err = rabbit.NewChannel()
	if err != nil {
		return nil, fmt.Errorf("Cant get new channel for RabbitMQ connection: %s", err)
	}

	return rabbit, nil
}

func (mq *RabbitMQ) NewChannel() (wabbit.Channel, error) {
	return mq.connection.Channel()
}

func (mq *RabbitMQ) SetExchange(config map[string]interface{}) (err error) {
	conf := mqConfig(config)

	if _, ok := mq.GetExchange(conf.GetString("name")); ok {
		return fmt.Errorf("Rabbit is already connected to %s exchange.", conf.GetString("name"))
	}

	exchange := &exchange{
		queues:       make(queues),
		producers:    make(producers),
		name:         conf.GetString("name"),
		exchangeType: conf.GetString("type"),
	}

	exchange.SetOptions(conf.GetStringMap("options"))

	if err = mq.channel.ExchangeDeclare(
		exchange.name,
		exchange.exchangeType,
		exchange.Options(),
	); err != nil {
		return fmt.Errorf("Can't decalare exchange: %s", err)
	}

	mq.AddExchange(exchange)

	return
}

func (mq *RabbitMQ) SetQueue(config map[string]interface{}) (err error) {
	conf := mqConfig(config)

	exchangeName := conf.GetString("exchange")
	exchange, ok := mq.exchanges[exchangeName]
	if !ok {
		return fmt.Errorf("You should set up exchange, before bind queue to %s exchange.", exchangeName)
	}

	queueName := conf.GetString("name")
	if _, ok := mq.exchanges[exchangeName].queues[queueName]; ok {
		return fmt.Errorf("Queue %s is already binded to %s exchange.", queueName, exchangeName)
	}

	queue := &queue{
		consumers:    make(consumers),
		name:         queueName,
		exchangeName: exchangeName,
		bindKey:      conf.GetString("bind_key"),
	}

	queue.SetOptions(conf.GetStringMap("options"))

	_, err = mq.channel.QueueDeclare(
		queue.name,
		queue.Options(),
	)
	if err != nil {
		return fmt.Errorf("Cant declare queue: %s (%s)", queue.name, err)
	}

	if err = mq.channel.QueueBind(
		queue.name,
		queue.bindKey,
		queue.exchangeName,
		queue.Options(),
	); err != nil {
		return fmt.Errorf("Queue Bind: %s", err)
	}

	exchange.AddQueue(queue)

	return
}

func (mq *RabbitMQ) RunConsumer(config map[string]interface{}, handler ConsumerHandler) (err error) {
	conf := mqConfig(config)

	consumer := &consumer{
		name:         conf.GetString("name"),
		exchangeName: conf.GetString("exchange"),
		queueName:    conf.GetString("queue"),
		handler:      handler,
		workersCount: conf.GetInt("workers"),
		errorChannel: mq.errorChannel,
	}

	channel, err := mq.NewChannel()
	if err != nil {
		return
	}

	if err = consumer.setChannel(channel); err != nil {
		return
	}

	consumer.SetOptions(conf.GetStringMap("options"))

	exchange, ok := mq.GetExchange(consumer.exchangeName)
	if !ok {
		err = fmt.Errorf("The exchange: %s is not yet initialized!", consumer.exchangeName)

		return
	}

	queue, ok := exchange.GetQueue(consumer.queueName)
	if !ok {
		err = fmt.Errorf("The queue: %s is not yet initialized for exchange: %s", consumer.queueName, consumer.exchangeName)

		return
	}

	if _, ok := queue.GetConsumer(consumer.queueName); ok {
		err = fmt.Errorf("Consumer: %s is already runned for %s queue.", consumer.name, queue.name)

		return
	}

	consumer.CreateWorkers()

	if err = consumer.RunWorkers(); err != nil {
		return
	}

	queue.AddConsumer(consumer)

	return
}

func (mq *RabbitMQ) RegisterProducer(config map[string]interface{}) (err error) {
	conf := mqConfig(config)

	producer := &producer{
		name:           conf.GetString("name"),
		exchangeName:   conf.GetString("exchange"),
		routingKey:     conf.GetString("routing_key"),
		publishChannel: make(chan []byte, conf.GetInt("buffer_size")),
		errorChannel:   mq.errorChannel,
	}

	producer.SetOptions(conf.GetStringMap("options"))

	exchange, ok := mq.GetExchange(producer.exchangeName)
	if !ok {
		err = fmt.Errorf("The exchange: %s is not yet initialized!", producer.exchangeName)

		return
	}

	channel, err := mq.NewChannel()
	if err != nil {
		return err
	}

	producer.setChannel(channel)

	go producer.worker()

	exchange.AddProducer(producer)

	return
}

func (mq *RabbitMQ) GetProducer(name string) (producer *producer, ok bool) {
	for _, exchange := range mq.exchanges {
		if producer, ok = exchange.GetProducer(name); ok {
			return
		}
	}

	return nil, false
}

func (mq *RabbitMQ) Errors() chan error {
	return mq.errorChannel
}

func (mq *RabbitMQ) connect() (err error) {
	if mq.connection, err = amqp.Dial(mq.dsn); err != nil {
		return
	}

	go func() {
		mq.errorChannel <- fmt.Errorf(
			"Ooops! RabbitMQ connection is closed: %s",
			<-mq.connection.NotifyClose(make(chan wabbit.Error)),
		)

		mq.reconnect()
	}()

	return
}

func (mq *RabbitMQ) reconnect() (ok bool) {
	mq.reconnectToRabbit(mq.reconnectDelay)

	for _, exchange := range mq.exchanges {
		for _, queue := range exchange.queues {
			for _, consumer := range queue.consumers {
				channel, err := mq.NewChannel()
				if err != nil {
					return
				}

				if err = consumer.setChannel(channel); err != nil {
					return
				}

				if err := consumer.RunWorkers(); err != nil {
					mq.reconnect()

					return
				}
			}
		}

		for _, producer := range exchange.producers {
			channel, err := mq.NewChannel()
			if err != nil {
				return
			}

			producer.setChannel(channel)
		}
	}

	return true
}

func (mq *RabbitMQ) reconnectToRabbit(delay time.Duration) {
	time.Sleep(delay)

	delay = increaseDelay(delay)

	var err error

	if err = mq.connect(); err != nil {
		mq.errorChannel <- fmt.Errorf(
			"Can't reconnect to RabbitMQ: %s. Next reconnection after: %.2f sec.",
			err, delay.Seconds(),
		)

		mq.reconnectToRabbit(delay)

		return
	}

	if mq.channel, err = mq.NewChannel(); err != nil {
		mq.errorChannel <- fmt.Errorf(
			"Can't reconnect to RabbitMQ: %s. Next reconnection after: %.2f sec.",
			err, delay.Seconds(),
		)

		mq.reconnectToRabbit(delay)

		return
	}
}

func increaseDelay(delay time.Duration) time.Duration {
	return delay + delay * reconnectTimeStep / 100
}

var globalRabbit *RabbitMQ

// InitGlobal initializes the connection to the RabbitMQ in variable
// `globalRabbit` so that you can use it anywhere in your application.
func InitGlobal(dsn string, reconnectDelay time.Duration) (err error) {
	globalRabbit, err = New(dsn, reconnectDelay)
	if err != nil {
		return err
	}

	return nil
}

// MQ return pointer to global RabbitMQ object. Ensure that
// before calling this function, you initialized the connection
// globally with calling of `InitGlobal` function.
func MQ() *RabbitMQ {
	return globalRabbit
}
