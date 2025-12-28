# orderedproc

## Summary

The orderedproc module facilitates parallel processing of data while ensuring the results are output in the same order as the input is provided.

This is performed using batching, which is why there is a required parameter called `batchSize`. `concurrency` is the number of goroutines that will be used for processing (will call your `procFunc`).

```go
import "github.com/cuberat-go/orderedproc"
```

```go
    procFunc := func(item int) int {
        return item * 2
    }

    proc := orderedproc.NewOrderedProcessor(procFunc,
        batchSize, concurrency)

    go func() {
        for i := range size {
            proc.Add(i)
        }
        proc.Done()
    }()

    got := slices.Collect(proc.Results())
```

Calling `Done()` tells the processor that you have finished adding data. The `Results()` method returns an interator over the results.

Example:

```go
    size := 7 // input will be 0..6

    procFunc := func(item int) int {
        return item * 2
    }

    proc := orderedproc.NewOrderedProcessor(procFunc, 5, 3)

    go func() {
        for i := range size {
            proc.Add(i)
        }
        proc.Done()
    }()

    got := slices.Collect(proc.Results())
    fmt.Printf("Results: %v\n", got)
    // Output:
    // Results: [0 2 4 6 8 10 12]
```

If you prefer to iterate over batches of results, call the `ResultBatches()` method.
