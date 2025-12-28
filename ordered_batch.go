package batchproc

import (
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
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

func logAttrReplacer() func(groups []string, a slog.Attr) slog.Attr {
	_, this_file, _, _ := runtime.Caller(0)
	orig_dir := filepath.Dir(this_file)

	return func(groups []string, a slog.Attr) slog.Attr {
		var err error
		if a.Key == slog.SourceKey {
			source := a.Value.Any().(*slog.Source)
			// source.File = filepath.Base(source.File)
			source.File, err = filepath.Rel(orig_dir, source.File)
			if err != nil {
				source.File = filepath.Base(source.File)
			}
		}
		return a
	}
}

// Creates a new OrderedBatchProcessor.
func NewOrderedBatchProcessor[IN_T, OUT_T any](
	procFunc ProcFunc[IN_T, OUT_T],
	batchSize int,
	concurrency int,
) *OrderedBatchProcessor[IN_T, OUT_T] {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelDebug)
	logger := slog.New(slog.NewTextHandler(
		os.Stderr, &slog.HandlerOptions{Level: logLevel, AddSource: true,
			ReplaceAttr: logAttrReplacer()}),
	)

	obp := &OrderedBatchProcessor[IN_T, OUT_T]{
		logger:            logger,
		procFunc:          procFunc,
		batchSize:         batchSize,
		concurrency:       concurrency,
		newItemChan:       make(chan *batchItem[IN_T, OUT_T], batchSize),
		toProcChan:        make(chan *batchItem[IN_T, OUT_T], batchSize),
		batchResponseChan: make(chan *OrderedBatch[IN_T, OUT_T], 2),
	}
	go obp.run()
	return obp
}

// Adds an item to be processed.
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) Add(item IN_T) {
	obp.newItemChan <- &batchItem[IN_T, OUT_T]{
		item:   item,
		logger: obp.logger,
	}
}

// Signals that no more items will be added.
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) Done() {
	close(obp.newItemChan)
}

func (obp *OrderedBatchProcessor[IN_T, OUT_T]) run() {
	var (
		itemProcWg               sync.WaitGroup
		responseProcWg           sync.WaitGroup
		responseChannel          chan *batchItem[IN_T, OUT_T]
		responseChannelWrittenWg *sync.WaitGroup
	)

	obp.logger.Debug("starting processing routines",
		"concurrency", obp.concurrency,
	)
	for range obp.concurrency {
		itemProcWg.Go(func() { obp.processItems() })
	}

	addedCount := obp.batchSize // Force initialization of first batch.
	batch_num := -1
	lastOrderIndex := -1
	for item := range obp.newItemChan {
		if addedCount >= obp.batchSize {
			if responseChannelWrittenWg != nil {
				// Close previous batch's response channel when all writes
				// to the response channel are done.
				obp.asyncWaitResponseCompletion(responseChannelWrittenWg,
					responseChannel, batch_num)
			}
			responseChannelWrittenWg = &sync.WaitGroup{}
			responseChannel = make(chan *batchItem[IN_T, OUT_T], obp.batchSize)
			batch_num++
			obp.logger.Debug("created response chan",
				"channel", responseChannel, "batch_num", batch_num)
			responseProcWg.Go(func() {
				obp.procResponseChannel(responseChannel)
			})

			addedCount = 0
			lastOrderIndex = -1
		}

		lastOrderIndex++
		item.orderIndex = lastOrderIndex

		obp.queueForProcessing(item, responseChannel,
			responseChannelWrittenWg)
		addedCount++
	}

	// Since we've exhausted newItemChan, and each item has been sent to
	// toProcChan, we can now close toProcChan to signal no more items
	// will be sent.
	close(obp.toProcChan)

	// Close the last batch's response channel when all writes to it are done.
	obp.waitResponseCompletion(responseChannelWrittenWg, responseChannel,
		batch_num)
	// responseChannelWrittenWg.Wait()
	// close(responseChannel)

	// Wait for processing routines to finish.
	itemProcWg.Wait()

	// Wait for response processing to finish.
	responseProcWg.Wait()

	// Signal that we've completed writing all results.
	close(obp.batchResponseChan)
}

func (obp *OrderedBatchProcessor[IN_T, OUT_T]) waitResponseCompletion(
	wg *sync.WaitGroup,
	rc chan *batchItem[IN_T, OUT_T],
	batch_num int,
) {
	obp.logger.Debug("waiting for response channel writes to complete",
		"channel", rc, "batch_num", batch_num)
	wg.Wait()
	close(rc)
	obp.logger.Debug("closed response channel",
		"channel", rc, "batch_num", batch_num)
}

func (obp *OrderedBatchProcessor[IN_T, OUT_T]) asyncWaitResponseCompletion(
	wg *sync.WaitGroup,
	rc chan *batchItem[IN_T, OUT_T],
	batch_num int,
) {
	go obp.waitResponseCompletion(wg, rc, batch_num)
}

func (obp *OrderedBatchProcessor[IN_T, OUT_T]) queueForProcessing(
	item *batchItem[IN_T, OUT_T],
	responseChannel chan *batchItem[IN_T, OUT_T],
	responseChannelWrittenWg *sync.WaitGroup,
) {
	item.responseChannel = responseChannel
	item.responseChannelWrittenWg = responseChannelWrittenWg
	responseChannelWrittenWg.Add(1)
	obp.toProcChan <- item // Send item for processing by processItems().
}

// Processes items (calls the function provided by the user).
func (obp *OrderedBatchProcessor[IN_T, OUT_T]) processItems() {
	for batchItem := range obp.toProcChan {
		if batchItem == nil {
			obp.logger.Debug("nil item in toProcChan")
		}

		// Call user func.
		resp := obp.procFunc(batchItem.Item())

		batchItem.result = resp

		// Send to be processed by procResponseChannel().
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
		if respItem == nil {
			obp.logger.Debug("nil item in response channel")
		}
		batch[respItem.orderIndex] = respItem
		obp.logger.Debug("added item to batch", "item", respItem.LogValue())
		if respItem.orderIndex > largestIndex {
			largestIndex = respItem.orderIndex
		}
		cnt++
	}

	if cnt == 0 {
		return
	}

	if largestIndex+1 < obp.batchSize {
		// batch = batch[:largestIndex+1]
		obp.logger.Debug("shrinking batch", "largestIndex", largestIndex,
			"batchSize", obp.batchSize, "count", cnt)
	} else {
		obp.logger.Debug("completed full batch", "largestIndex", largestIndex,
			"batchSize", obp.batchSize, "count", cnt)
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
					continue
				}
				if !yield(item.Result()) {
					return
				}
			}
		}
	}
}
