package batchproc

import (
	// Built-in/core modules.
	"iter"
	"log/slog"
	"os"
	"sync"
	// Third-party modules.
	// Generated code.
	// First-party modules.
)

type ProcFunc[I, O any] func(item *BatchItem[I, O]) O

type OrderedBatchProcessor[I, O any] struct {
	// Maximum size of each batch.
	batchSize int

	// Index of the last item added to the current batch.
	lastOrderIndex int

	// Number of concurrent worker routines.
	concurrency int

	// Channel to add new items to be processed. AddItem() writes to this
	// channel.
	addChannel chan *BatchItem[I, O]

	// Channel to return completed batches in order.
	batchReturnChannel chan *OrderedBatch[I, O]

	// Logger instance for debugging.
	logger *slog.Logger

	// Channel to send items to be processed by the provided user function.
	procChannel chan *BatchItem[I, O]

	// User-provided processing function.
	procFunc ProcFunc[I, O]
}

// Creates a new OrderedBatchProcessor.
func NewOrderedBatchProcessor[I, O any](
	procFunc ProcFunc[I, O],
	batchSize int,
	concurrency int,
) *OrderedBatchProcessor[I, O] {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelError)
	logger := slog.New(slog.NewTextHandler(
		os.Stderr, &slog.HandlerOptions{Level: logLevel}),
	)

	obp := &OrderedBatchProcessor[I, O]{
		logger:             logger,
		procFunc:           procFunc,
		batchSize:          batchSize,
		lastOrderIndex:     -1,
		concurrency:        concurrency,
		addChannel:         make(chan *BatchItem[I, O], batchSize),
		batchReturnChannel: make(chan *OrderedBatch[I, O], 2),
		procChannel:        make(chan *BatchItem[I, O], batchSize),
	}
	go obp.run()
	return obp
}

// Adds an item to be processed.
func (obp *OrderedBatchProcessor[I, O]) AddItem(item I) {
	obp.lastOrderIndex++
	if obp.lastOrderIndex >= obp.batchSize {
		obp.lastOrderIndex = 0
	}
	obp.addChannel <- &BatchItem[I, O]{
		item:       item,
		orderIndex: obp.lastOrderIndex,
		logger:     obp.logger,
	}
}

// Signals that no more items will be added.
func (obp *OrderedBatchProcessor[I, O]) Finish() {
	obp.logger.Debug("Finish() called")
	close(obp.addChannel)
}

func (obp *OrderedBatchProcessor[I, O]) run() {
	var (
		workerWg                 sync.WaitGroup
		responseProcWg           sync.WaitGroup
		responseChannel          chan *BatchItem[I, O]
		responseChannelWrittenWg *sync.WaitGroup
	)

	obp.logger.Debug("run() started")

	for range obp.concurrency {
		workerWg.Go(func() { obp.processItems() })
	}

	addedCount := obp.batchSize // Force initialization of first batch.
	for batchItem := range obp.addChannel {
		if addedCount >= obp.batchSize {
			obp.logger.Debug("starting new batch", "batchSize", obp.batchSize)
			if responseChannelWrittenWg != nil {
				go func(wg *sync.WaitGroup, rc chan *BatchItem[I, O]) {
					wg.Wait()
					close(rc)
				}(responseChannelWrittenWg, responseChannel)
			}
			responseChannelWrittenWg = &sync.WaitGroup{}
			responseChannel = make(chan *BatchItem[I, O], obp.batchSize)
			responseProcWg.Go(func() {
				obp.procResponseChannel(responseChannel)
			})

			addedCount = 0
		}
		batchItem.responseChannel = responseChannel
		batchItem.responseChannelWrittenWg = responseChannelWrittenWg
		responseChannelWrittenWg.Add(1)
		obp.procChannel <- batchItem
		addedCount++
	}

	close(obp.procChannel)

	responseChannelWrittenWg.Wait()
	close(responseChannel)

	slog.Debug("waiting for workers to finish")

	// Wait for the worker routines to finish.
	workerWg.Wait()

	obp.logger.Debug("waiting for response processing to finish")

	// Wait for response processing to finish.
	responseProcWg.Wait()

	obp.logger.Debug("all done, closing batch return channel")

	// Signal that we've completed all batches.
	close(obp.batchReturnChannel)
}

// Processes items (calls the function provided by the user).
func (obp *OrderedBatchProcessor[I, O]) processItems() {
	for batchItem := range obp.procChannel {
		resp := obp.procFunc(batchItem)
		batchItem.result = resp
		batchItem.responseChannel <- batchItem
		batchItem.responseChannelWrittenWg.Done()
	}
}

// Processes the responses for a given batch, sending them in an ordered batch
// to the batch return channel.
func (obp *OrderedBatchProcessor[I, O]) procResponseChannel(
	responseChannel chan *BatchItem[I, O],
) {
	largestIndex := -1
	batch := make([]*BatchItem[I, O], obp.batchSize)

	cnt := 0
	for respItem := range responseChannel {
		batch[respItem.orderIndex] = respItem
		if respItem.orderIndex > largestIndex {
			largestIndex = respItem.orderIndex
		}
		cnt++
	}

	if cnt == 0 {
		return
	}

	if largestIndex+1 < obp.batchSize {
		batch = batch[:largestIndex+1]
	}

	obp.batchReturnChannel <- &OrderedBatch[I, O]{Items: batch}
}

// Return individual items from batch results
func (obp *OrderedBatchProcessor[I, O]) Results() iter.Seq[O] {
	return func(yield func(O) bool) {
		for batch := range obp.batchReturnChannel {
			for _, item := range batch.Items {
				if item == nil {
					return
				}
				if !yield(item.Result()) {
					return
				}
			}
		}
	}
}

type OrderedBatch[I, O any] struct {
	Items []*BatchItem[I, O]
}

type BatchItem[I, O any] struct {
	item                     I
	logger                   *slog.Logger
	result                   O
	orderIndex               int
	responseChannel          chan *BatchItem[I, O]
	responseChannelWrittenWg *sync.WaitGroup
}

func (item *BatchItem[I, O]) Item() I {
	return item.item
}

func (item *BatchItem[I, O]) Result() O {
	return item.result
}

func (item *BatchItem[I, O]) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("orderIndex", item.orderIndex),
		slog.Any("item", item.item),
		slog.Any("result", item.result),
	)
}
