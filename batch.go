package batchproc

import (
	"log/slog"
	"sync"
)

type OrderedBatch[I, O any] struct {
	Items []*batchItem[I, O]
}

type batchItem[I, O any] struct {
	item                     I
	logger                   *slog.Logger
	result                   O
	orderIndex               int
	responseChannel          chan *batchItem[I, O]
	responseChannelWrittenWg *sync.WaitGroup
}

func (item *batchItem[I, O]) Item() I {
	return item.item
}

func (item *batchItem[I, O]) Result() O {
	return item.result
}

func (item *batchItem[I, O]) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("orderIndex", item.orderIndex),
		slog.Any("item", item.item),
		slog.Any("result", item.result),
	)
}
