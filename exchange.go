package gogorabbit

type exchanges map[string]*exchange

func (exchanges exchanges) AddExchange(exchange *exchange) {
	exchanges[exchange.name] = exchange
}

func (exchanges exchanges) GetExchange(name string) (exchange *exchange, ok bool) {
	exchange, ok = exchanges[name]

	return
}

type exchange struct {
	options
	queues
	producers
	name         string
	exchangeType string
}
