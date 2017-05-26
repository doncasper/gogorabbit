package gogorabbit

type exchanges map[string]*exchange

func (e exchanges) AddExchange(exchange *exchange) {
	e[exchange.name] = exchange
}

func (e exchanges) GetExchange(name string) (exchange *exchange, ok bool) {
	exchange, ok = e[name]

	return
}

type exchange struct {
	options
	queues
	producers
	name         string
	exchangeType string
}
