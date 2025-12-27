package batchproc_test

import (
	// Built-in/core modules.
	"log/slog"
	"os"
	"slices"
	"testing"

	// Third-party modules.
	"github.com/stretchr/testify/assert"

	// First-party modules.
	"github.com/cuberat-go/batchproc"
)

func TestOrderedBatch(t *testing.T) {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelInfo)
	slog.SetDefault(slog.New(slog.NewTextHandler(
		os.Stderr, &slog.HandlerOptions{Level: logLevel}),
	))

	type testCase struct {
		batchSize   int
		concurrency int
		rangeLimit  int
		name        string
	}

	testCases := []testCase{
		{batchSize: 10, concurrency: 3, rangeLimit: 5, name: "lt batchsize"},
		{batchSize: 10, concurrency: 3, rangeLimit: 10, name: "eq batchsize"},
		{batchSize: 10, concurrency: 3, rangeLimit: 12, name: "gt batchsize"},
		// {batchSize: 5, concurrency: 3, rangeLimit: 20, name: "gt 2 batches"},
		// {batchSize: 10, concurrency: 1, rangeLimit: 12, name: "concurrency 1"},
		{batchSize: 3, concurrency: 3, rangeLimit: 1, name: "size 1"},
		// {batchSize: 7, concurrency: 10, rangeLimit: 7,
		// 	name: "concurrency gt size"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testOne(t, tc.batchSize, tc.concurrency, tc.rangeLimit)
		})
	}
}

func testOne(t *testing.T, batchSize int, concurrency int, rangeLimit int) {

	procFunc := func(item int) int {
		return item * 2
	}
	proc := batchproc.NewOrderedBatchProcessor(procFunc, batchSize, concurrency)

	go func() {
		for i := range rangeLimit {
			proc.Add(i)
		}
		proc.Done()
	}()
	expected := make([]int, rangeLimit)
	for i := range rangeLimit {
		expected[i] = i * 2
	}

	slog.Debug("Collecting results")

	got := slices.Collect(proc.Results())

	assert.Equal(t, expected, got)
}
