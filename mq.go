package gogorabbit

import (
	"fmt"
	"sync"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/spf13/viper"
)

type ConsumerHandler func([]byte) error

type DoneChannel chan struct{}

type RabbitMQ struct {
	sync.Mutex
	exchanges
	dsn        string
	reconDelay time.Duration // Seconds
	connection wabbit.Conn
	channel    wabbit.Channel
}

func New(config *viper.Viper) (*RabbitMQ, error) {
	var err error

	rabbit := &RabbitMQ{
		dsn:        config.GetString("dsn"),
		reconDelay: config.GetDuration("reconnection_delay") * time.Second,
		exchanges:  make(exchanges),
	}

	err = rabbit.connect()
	if err != nil {
		return nil, fmt.Errorf("Error dialing RabbitMQ connection: %s", err)
	}

	rabbit.channel, err = rabbit.NewChannel()
	if err != nil {
		return nil, fmt.Errorf("Cant get new channel for RabbitMQ connection: %s", err)
	}

	if err := rabbit.channel.Qos(1, 0, false); err != nil {
		return nil, err
	}

	return rabbit, nil
}

func (rabbit *RabbitMQ) NewChannel() (wabbit.Channel, error) {
	return rabbit.connection.Channel()
}

func (rabbit *RabbitMQ) SetExchange(config *viper.Viper) (err error) {
	if _, ok := rabbit.GetExchange(config.GetString("name")); ok {
		return fmt.Errorf("Rabbit is already connected to %s exchange.", config.GetString("name"))
	}

	exchange := &exchange{
		name:         config.GetString("name"),
		exchangeType: config.GetString("type"),
		queues:       make(queues),
		producers:    make(producers),
	}

	exchange.SetOptions(config.GetStringMap("options"))

	if err = rabbit.channel.ExchangeDeclare(
		exchange.name,
		exchange.exchangeType,
		exchange.Options(),
	); err != nil {
		return fmt.Errorf("Can't decalare exchange: %s", err)
	}

	rabbit.AddExchange(exchange)

	return
}

func (rabbit *RabbitMQ) SetQueue(config *viper.Viper) (err error) {
	exchangeName := config.GetString("exchange")
	exchange, ok := rabbit.exchanges[exchangeName]
	if !ok {
		return fmt.Errorf("You should set up exchange, before bind queue to %s exchange.", exchangeName)
	}

	queueName := config.GetString("name")
	if _, ok := rabbit.exchanges[exchangeName].queues[queueName]; ok {
		return fmt.Errorf("Queue %s is already binded to %s exchange.", queueName, config.GetString("exchange"))
	}

	queue := &queue{
		name:         queueName,
		exchangeName: exchangeName,
		bindKey:      config.GetString("bind_key"),
		consumers:    make(consumers),
	}

	queue.SetOptions(config.GetStringMap("options"))

	_, err = rabbit.channel.QueueDeclare(
		queue.name,
		queue.Options(),
	)
	if err != nil {
		return fmt.Errorf("Cant declare queue: %s (%s)", queue.name, err)
	}

	if err = rabbit.channel.QueueBind(
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

func (rabbit *RabbitMQ) RunConsumer(config *viper.Viper, handler ConsumerHandler) (err error) {
	consumer := &consumer{
		name:         config.GetString("name"),
		exchangeName: config.GetString("exchange"),
		queueName:    config.GetString("queue"),
		handler:      handler,
		workersCount: config.GetInt("workers"),
	}

	channel, err := rabbit.NewChannel()
	if err != nil {
		return
	}
	consumer.channel = channel

	consumer.SetOptions(config.GetStringMap("options"))

	exchange, ok := rabbit.GetExchange(consumer.exchangeName)
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

func (rabbit *RabbitMQ) RegisterProducer(config *viper.Viper) (err error) {
	producer := &producer{
		name:           config.GetString("name"),
		exchangeName:   config.GetString("exchange"),
		routingKey:     config.GetString("routing_key"),
		publishChannel: make(chan []byte, config.GetInt("buffer_size")),
	}

	producer.SetOptions(config.GetStringMap("options"))

	exchange, ok := rabbit.GetExchange(producer.exchangeName)
	if !ok {
		err = fmt.Errorf("The exchange: %s is not yet initialized!", producer.exchangeName)

		return
	}

	channel, err := rabbit.NewChannel()
	if err != nil {
		return err
	}

	producer.setChannel(channel)

	go producer.worker()

	exchange.AddProducer(producer)

	return
}

func (rabbit *RabbitMQ) GetProducer(name string) (producer *producer, ok bool) {
	for _, exchange := range rabbit.exchanges {
		if producer, ok = exchange.GetProducer(name); ok {
			return
		}
	}

	return nil, false
}

func (rabbit *RabbitMQ) connect() (err error) {
	if rabbit.connection, err = amqp.Dial(rabbit.dsn); err != nil {
		return
	}

	go func() {
		rabbit.reconnect()
	}()

	return
}

func (rabbit *RabbitMQ) reconnect() (ok bool) {
	rabbit.reconnectToRabbit(rabbit.reconDelay)

	// TODO: Refactor this after implementing producers!
	for _, exchange := range rabbit.exchanges {
		for _, queue := range exchange.queues {
			for _, consumer := range queue.consumers {
				consumer.channel = rabbit.channel

				if err := consumer.RunWorkers(); err != nil {
					consumer.logger.Warnf("Cant rerun consumer workers: %s", err)
					rabbit.reconnect()

					return
				}
			}
		}
	}

	return true
}

func (rabbit *RabbitMQ) reconnectToRabbit(delay time.Duration) {
	time.Sleep(delay)

	delay += time.Millisecond * 500

	var err error

	if err = rabbit.connect(); err != nil {
		rabbit.reconnectToRabbit(delay)

		return
	}

	if rabbit.channel, err = rabbit.NewChannel(); err != nil {
		rabbit.reconnectToRabbit(delay)

		return
	}
}

var globalRabbit *RabbitMQ

// InitGlobal initializes the connection to the RabbitMQ in variable
// `globalRabbit` so that you can use it anywhere in your application.
// But, this is a bad practice, try to avoid global variables if you can.
func InitGlobal(config *viper.Viper) (err error) {
	globalRabbit, err = New(config)
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
