// Copyright 2014 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package mapreduce

import (
	"errors"
	"testing"

	"github.com/maruel/ut"
)

type mapperImpl struct {
	t               *testing.T
	returnInterface bool
	err             error
}

func (m *mapperImpl) Map(io MapIO) error {
	if m.t != nil {
		m.t.Fatal("This wasn't expected")
	} else if m.err != nil {
		return m.err
	} else if m.returnInterface {
		// Return a non-marshallable interface.
		io.Emit(io.MapKey()+".1", make(chan string))
		return nil
	}
	io.Emit(io.MapKey()+".1", 1)
	return nil
}

func TestMapReduceOne(t *testing.T) {
	cache := &MappingCache{}
	cache.SetValueType(0)
	mapper := &mapperImpl{}
	out := make(chan KeyValue, 1)
	in := make(chan string)
	go func() {
		in <- "A"
		close(in)
	}()
	perf := &PerfStats{}
	MapReduce(in, out, make(chan error), cache, perf, mapper, &ReducePassThrough{})

	i := <-out
	ut.AssertEqual(t, "A.1", i.Key)
	ut.AssertEqual(t, 1, i.Value.(int))
	_, ok := <-out
	ut.AssertEqual(t, false, ok)
	ut.AssertEqual(t, 0, perf.MappersRunning())
	ut.AssertEqual(t, 0, perf.ReducersRunning())
	ut.AssertEqual(t, 0, perf.CacheHits())
	ut.AssertEqual(t, 1, perf.CacheMisses())

	// Again, this time with a cache hit. If the mapper would be called, it would
	// crash.
	mapper = &mapperImpl{t: t}
	out = make(chan KeyValue, 1)
	in = make(chan string)
	go func() {
		in <- "A"
		close(in)
	}()
	perf = &PerfStats{}
	MapReduce(in, out, make(chan error), cache, perf, mapper, &ReducePassThrough{})
	i = <-out
	ut.AssertEqual(t, "A.1", i.Key)
	ut.AssertEqual(t, 1, i.Value.(int))
	_, ok = <-out
	ut.AssertEqual(t, false, ok)
	ut.AssertEqual(t, 0, perf.MappersRunning())
	ut.AssertEqual(t, 0, perf.ReducersRunning())
	ut.AssertEqual(t, 1, perf.CacheHits())
	ut.AssertEqual(t, 0, perf.CacheMisses())
}

func TestMapReduceErrorMapper(t *testing.T) {
	errChan := make(chan error, 1)
	in := make(chan string)
	go func() {
		in <- "A"
		close(in)
	}()
	MapReduce(in, make(chan KeyValue), errChan, nil, nil, &mapperImpl{err: errors.New("Oh")}, &ReducePassThrough{})

	err := <-errChan
	ut.AssertEqual(t, "failed to map A: Oh", err.Error())
}

func TestMapReduceErrorEmitCacheType(t *testing.T) {
	cache := &MappingCache{}
	cache.SetValueType("")
	errChan := make(chan error, 1)
	out := make(chan KeyValue, 1)
	in := make(chan string)
	go func() {
		in <- "A"
		close(in)
	}()
	MapReduce(in, out, errChan, cache, nil, &mapperImpl{}, &ReducePassThrough{})

	err := <-errChan
	ut.AssertEqual(t, "expected type string, got int", err.Error())

	i := <-out
	ut.AssertEqual(t, "A.1", i.Key)
	ut.AssertEqual(t, 1, i.Value.(int))
	_, ok := <-out
	ut.AssertEqual(t, false, ok)
}

func TestMapReduceErrorEmitMarshal(t *testing.T) {
	cache := &MappingCache{}
	cache.SetValueType(0)
	errChan := make(chan error, 2)
	out := make(chan KeyValue, 1)
	in := make(chan string)
	go func() {
		in <- "A"
		close(in)
	}()
	MapReduce(in, out, errChan, cache, nil, &mapperImpl{returnInterface: true}, &ReducePassThrough{})

	err := <-errChan
	ut.AssertEqual(t, "expected type int, got chan string", err.Error())

	i := <-out
	ut.AssertEqual(t, "A.1", i.Key)
	_, ok := i.Value.(chan string)
	ut.AssertEqual(t, true, ok)
	_, ok = <-out
	ut.AssertEqual(t, false, ok)
}
