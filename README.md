# orderedproc

## Summary

```go
import "github.com/cuberat-go/orderedproc"
```

The orderedproc module facilitates parallel processing of data while maintaining the order of the input. That is, the output is provided in the same order as the input was provided.

This is performed using batching, which is why there is a required parameter called `batchSize`.
