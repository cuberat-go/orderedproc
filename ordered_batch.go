package batchproc

import (
	"iter"
	"log/slog"
	"os"
	"sync"
)

// Type definition for the user-provided processing function.
type ProcFunc[IN_T, OUT_T any] func(IN_T) OUT_T

type OrderedBatchProcessor[IN_T, OUT_T any] struct {
	// Maximum size of each batch.
	batchSize int

	// Index of the last item added to the current batch.
	lastOrderIndex int

	// Number of concurrent worker routines.
	concurrency int

	// Channel to add new items to be processed. Add() writes to this
	// channel. run() reads from it.
	newItemChan chan *batchItem[IN_T, OUT_T]

	// Channel containing items to be processed by the user-provided processing
	// function. Written to by queueForProcessing(), read from by
	// processItems().
	toProcChan chan *batchItem[IN_T, OUT_T]

	// Channel containing batches of responses in order. Written to by
	// procResponseChannel(), read from by Results().
	batchResponseChan chan *OrderedBatch[IN_T, OUT_T]

	// Logger instance for debugging.
	logger *slog.Logger

	// User-provided processing function.
	procFunc ProcFunc[IN_T, OUT_T]
}

// Creates a new OrderedBatchProcessor.
func NewOrderedBatchProcessor[IN_T, OUT_T any](
	procFunc ProcFunc[IN_T, OUT_T],
	batchSize int,
	concurrency int,
) *OrderedBatchProcessor[IN_T, OUT_T] {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelError)
	logger := slog.New(slog.NewTextHandler(
		os.Stderr, &slog.HandlerOptions{Level: logLevel}),
	)

	obp := &OrderedBatchProcessor[IN_T, OUT_T]{
		logger:            logger,
		procFunc:          procFunc,
		batchSize:         batchSize,
		lastOrderIndex:    -1,
		concurrency:       concurrency,
		newItemChan:       make(chan *batchItem[IN_T, OUT_T], batchSize),
		batchResponseChan: make(chan *OrderedBatch[IN_T, OUT_T], 2),
		toProcChan:        make(chan *batchItem[IN_T, OUT_T], batchSize),
	}
	go obp.run()
	return obp
}

// Adds an item to be processed.
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) Add(item IN_T) {
	obp.lastOrderIndex++
	if obp.lastOrderIndex >= obp.batchSize {
		obp.lastOrderIndex = 0
	}
	obp.newItemChan <- &batchItem[IN_T, OUT_T]{
		item:       item,
		orderIndex: obp.lastOrderIndex,
		logger:     obp.logger,
	}
}

// Signals that no more items will be added.
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) Done() {
	obp.logger.Debug("Finish() called")
	close(obp.newItemChan)
}

func (obp *OrderedBatchProcessor[IN_T, OUT_T]) run() {
	var (
		workerWg                 sync.WaitGroup
		responseProcWg           sync.WaitGroup
		responseChannel          chan *batchItem[IN_T, OUT_T]
		responseChannelWrittenWg *sync.WaitGroup
	)

	for range obp.concurrency {
		workerWg.Go(func() { obp.processItems() })
	}

	addedCount := obp.batchSize // Force initialization of first batch.
	for item := range obp.newItemChan {
		if addedCount >= obp.batchSize {
			if responseChannelWrittenWg != nil {
				obp.closeResponseChannelWhenDone(responseChannelWrittenWg,
					responseChannel)
			}
			responseChannelWrittenWg = &sync.WaitGroup{}
			responseChannel = make(chan *batchItem[IN_T, OUT_T], obp.batchSize)
			responseProcWg.Go(func() {
				obp.procResponseChannel(responseChannel)
			})

			addedCount = 0
		}

		obp.queueForProcessing(item, responseChannel,
			responseChannelWrittenWg)
		addedCount++
	}

	close(obp.toProcChan)

	responseChannelWrittenWg.Wait()
	close(responseChannel)

	// Wait for worker routines to finish.
	workerWg.Wait()

	// Wait for response processing to finish.
	responseProcWg.Wait()

	// Signal that we've completed all batches.
	close(obp.batchResponseChan)
}

func (obp *OrderedBatchProcessor[IN_T, OUT_T]) closeResponseChannelWhenDone(
	wg *sync.WaitGroup,
	rc chan *batchItem[IN_T, OUT_T],
) {
	go func(wg *sync.WaitGroup, rc chan *batchItem[IN_T, OUT_T]) {
		wg.Wait()
		close(rc)
	}(wg, rc)
}

func (obp *OrderedBatchProcessor[IN_T, OUT_T]) queueForProcessing(
	batchItem *batchItem[IN_T, OUT_T],
	responseChannel chan *batchItem[IN_T, OUT_T],
	responseChannelWrittenWg *sync.WaitGroup,
) {
	batchItem.responseChannel = responseChannel
	batchItem.responseChannelWrittenWg = responseChannelWrittenWg
	responseChannelWrittenWg.Add(1)
	obp.toProcChan <- batchItem
}

// Processes items (calls the function provided by the user).
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) processItems() {
	for batchItem := range obp.toProcChan {
		resp := obp.procFunc(batchItem.Item())
		batchItem.result = resp
		batchItem.responseChannel <- batchItem
		batchItem.responseChannelWrittenWg.Done()
	}
}

// Processes the responses for a given batch, sending them in an ordered batch
// to the batch return channel.
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) procResponseChannel(
	responseChannel chan *batchItem[IN_T, OUT_T],
) {
	largestIndex := -1
	batch := make([]*batchItem[IN_T, OUT_T], obp.batchSize)

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

	obp.batchResponseChan <- &OrderedBatch[IN_T, OUT_T]{Items: batch}
}

// Return individual items from batch results
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) Results() iter.Seq[OUT_T] {
	return func(yield func(OUT_T) bool) {
		batch_num := -1
		for batch := range obp.batchResponseChan {
			batch_num++
			for item_num, item := range batch.Items {
				if item == nil {
					obp.logger.Error("nil item in batch",
						slog.Int("batch_num", batch_num),
						slog.Int("item_num", item_num))
					return
				}
				if !yield(item.Result()) {
					return
				}
			}
		}
	}
}
