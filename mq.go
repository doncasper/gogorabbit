package gogorabbit

import (
	"fmt"
	"sync"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

type ConsumerHandler func([]byte) error

type DoneChannel chan struct{}

type RabbitMQ struct {
	sync.Mutex
	exchanges
	dsn          string
	reconDelay   time.Duration // Seconds
	connection   wabbit.Conn
	channel      wabbit.Channel
	logger       logrus.FieldLogger
}

func New(config *viper.Viper) (*RabbitMQ, error) {
	var err error

	rabbit := &RabbitMQ{
		dsn:        config.GetString("dsn"),
		reconDelay: config.GetDuration("reconnection_delay") * time.Second,
		exchanges:  make(exchanges),
		logger:     logrus.WithField("logger", "gogorabbit"),
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
	rabbit.logger.Debug("Get channel for RabbitMQ connection!")
	return rabbit.connection.Channel()
}

func (rabbit *RabbitMQ) SetExchange(config *viper.Viper) (err error) {
	rabbit.logger.Debugf("Start setting exchange: %s", config.GetString("name"))

	if _, ok := rabbit.GetExchange(config.GetString("name")); ok {
		return fmt.Errorf("Rabbit is already connected to %s exchange.", config.GetString("name"))
	}

	exchange := &exchange{
		Name:   config.GetString("name"),
		Type:   config.GetString("type"),
		queues: make(queues),
	}

	exchange.SetOptions(config.GetStringMap("options"))

	if err = rabbit.channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
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

	rabbit.logger.Debugf("Start setting queue %s for exchange: %s", queueName, exchangeName)

	queue := &queue{
		Name:         queueName,
		ExchangeName: exchangeName,
		BindKey:      config.GetString("bind_key"),
		consumers:    make(consumers),
	}

	queue.SetOptions(config.GetStringMap("options"))

	rabbit.logger.Debugf("Declare queue %s in RabbitMQ channel!", queue.Name)
	_, err = rabbit.channel.QueueDeclare(
		queue.Name,
		queue.Options(),
	)
	if err != nil {
		return fmt.Errorf("Cant declare queue: %s (%s)", queue.Name, err)
	}

	rabbit.logger.Debugf("Bind queue %s to RabbitMQ channel!", queue.Name)
	if err = rabbit.channel.QueueBind(
		queue.Name,
		queue.BindKey,
		queue.ExchangeName,
		queue.Options(),
	); err != nil {
		return fmt.Errorf("Queue Bind: %s", err)
	}

	exchange.AddQueue(queue)

	return
}

func (rabbit *RabbitMQ) RunConsumer(config *viper.Viper, handler ConsumerHandler) (err error) {
	consumer := &consumer{
		channel:      rabbit.channel,
		Name:         config.GetString("name"),
		ExchangeName: config.GetString("exchange"),
		QueueName:    config.GetString("queue"),
		Handler:      handler,
		WorkersCount: config.GetInt("workers"),
	}

	consumer.logger = rabbit.logger.WithField("logger", "consumer."+consumer.Name)

	consumer.SetOptions(config.GetStringMap("options"))

	exchange, ok := rabbit.GetExchange(consumer.ExchangeName)
	if !ok {
		err = fmt.Errorf("The exchange: %s is not yet initialized!", consumer.ExchangeName)

		return
	}

	queue, ok := exchange.GetQueue(consumer.QueueName)
	if !ok {
		err = fmt.Errorf("The queue: %s is not yet initialized for exchange: %s", consumer.QueueName, consumer.ExchangeName)

		return
	}

	if _, ok := queue.GetConsumer(consumer.QueueName); ok {
		err = fmt.Errorf("Consumer: %s is already runned for %s queue.", consumer.Name, queue.Name)

		return
	}

	consumer.CreateWorkers()

	if err = consumer.RunWorkers(); err != nil {
		return
	}

	queue.AddConsumer(consumer)

	return
}

func (rabbit *RabbitMQ) connect() (err error) {
	rabbit.logger.Debugf("Start dialing connection to RabbitMQ: %s", rabbit.dsn)

	if rabbit.connection, err = amqp.Dial(rabbit.dsn); err != nil {
		return
	}

	go func() {
		rabbit.logger.Warnf("Ooops! RabbitMQ connection is closed: %s", <-rabbit.connection.NotifyClose(make(chan wabbit.Error)))

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
		rabbit.logger.Warnf("Can't reconnect to RabbitMQ: %s", err)
		rabbit.logger.Infof("Sleep before next reconnection: %v sec.", delay.Seconds())

		rabbit.reconnectToRabbit(delay)

		return
	}

	if rabbit.channel, err = rabbit.NewChannel(); err != nil {
		rabbit.logger.Warnf("Can't reopen RabbitMQ channel: %s", err)
		rabbit.logger.Infof("Sleep before next reconnection: %v sec.", delay.Seconds())

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
