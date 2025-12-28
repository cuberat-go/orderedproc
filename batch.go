package orderedproc

import (
	"log/slog"
	"sync"
)

type OrderedBatch[IN_T, OUT_T any] struct {
	// Items []*batchItem[IN_T, OUT_T]
	Items []OUT_T
}

type batchItem[IN_T, OUT_T any] struct {
	item                     IN_T
	logger                   *slog.Logger
	result                   OUT_T
	orderIndex               int
	responseChannel          chan *batchItem[IN_T, OUT_T]
	responseChannelWrittenWg *sync.WaitGroup
}

func (item *batchItem[IN_T, OUT_T]) Item() IN_T {
	return item.item
}

func (item *batchItem[IN_T, OUT_T]) Result() OUT_T {
	return item.result
}

func (item *batchItem[IN_T, OUT_T]) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("orderIndex", item.orderIndex),
		slog.Any("item", item.item),
		slog.Any("result", item.result),
	)
}
