package orderedproc_test

import (
	// Built-in/core modules.
	"fmt"
	"log/slog"
	"os"
	"slices"
	"testing"

	// Third-party modules.
	"github.com/stretchr/testify/assert"

	// First-party modules.
	"github.com/cuberat-go/orderedproc"
)

func TestOrderedBatch(t *testing.T) {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelDebug)
	slog.SetDefault(slog.New(slog.NewTextHandler(
		os.Stderr, &slog.HandlerOptions{Level: logLevel}),
	))

	type testCase struct {
		batchSize   int
		concurrency int
		size        int
		name        string
	}

	testCases := []testCase{
		{batchSize: 10, concurrency: 3, size: 5, name: "lt batchsize"},
		{batchSize: 10, concurrency: 3, size: 10, name: "eq batchsize"},
		{batchSize: 10, concurrency: 3, size: 12, name: "gt batchsize"},
		{batchSize: 5, concurrency: 3, size: 20, name: "gt 2 batches"},
		{batchSize: 10, concurrency: 1, size: 12, name: "concurrency 1"},
		{batchSize: 3, concurrency: 3, size: 1, name: "size 1"},
		{batchSize: 7, concurrency: 10, size: 7,
			name: "concurrency gt size"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testOne(t, tc.batchSize, tc.concurrency, tc.size)
		})
	}
}

func TestExample(t *testing.T) {
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
}

func testOne(t *testing.T, batchSize int, concurrency int, size int) {
	expected := make([]int, size)
	for i := range size {
		expected[i] = i * 2
	}

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

	assert.Equal(t, expected, got)
}
