// Copyright 2014 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

// Package mapreduce is a single-process generic trivial mapreducer with cache
// support.
//
// This is mostly useful when harvesting a remote site through a Mapper then
// reducing the data for analysis. The harvested data will be cached in
// MappingCache, which permits much faster re-execution.
package mapreduce

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// Public API.

// MapIO is the argument to the mapper.
type MapIO interface {
	MapKey() string
	Emit(reduceKey string, reduceValue interface{})
}

// ReduceIO is the argument to the reducer.
type ReduceIO interface {
	ReduceKey() string
	ReduceValues() <-chan interface{}
	Output(finalKey string, finalValue interface{})
}

// Mapper is what generates data from keys.
type Mapper interface {
	Map(m MapIO) error
}

// Reducer is what reduces data from what was generated by the Mapper.
type Reducer interface {
	Reduce(r ReduceIO) error
}

// MappingCache caches all the data. It is serializable.
type MappingCache struct {
	lock      sync.Mutex
	valueType reflect.Type // Do not export so it is not serialized; reflect.Type can't be serialized.
	Data      map[string]*cacheValues
}

// SetValueType must be called before usage.
func (c *MappingCache) SetValueType(value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.valueType = reflect.TypeOf(value)
}

// KeyValue is a key-value pair.
type KeyValue struct {
	Key   string
	Value interface{}
}

// PerfStats stores the performance statistics of mapreduce execution and the
// cache hit and miss rate.
type PerfStats struct {
	mappersRunning  int64
	reducersRunning int64
	cacheHits       int64
	cacheMisses     int64
}

// MappersRunning returns the number of mappers currently running.
func (p *PerfStats) MappersRunning() int {
	return int(atomic.LoadInt64(&p.mappersRunning))
}

// ReducersRunning returns the number of reduces currently running.
func (p *PerfStats) ReducersRunning() int {
	return int(atomic.LoadInt64(&p.reducersRunning))
}

// CacheHits returns the number of mapper that were skipped becayse of cache
// hit found from MappingCache when running MapReduce.
func (p *PerfStats) CacheHits() int {
	return int(atomic.LoadInt64(&p.cacheHits))
}

// CacheMisses returns the number of mapper that were run because the key was
// not in the cache.
func (p *PerfStats) CacheMisses() int {
	return int(atomic.LoadInt64(&p.cacheMisses))
}

// MapReduce runs a complete map reduce and returns when done.
//
// It exhausts generator and closes out once done. Any error is sent to
// errChan. The optional cache is used to skip mapping steps. Perf stats are
// updated live to perf.
func MapReduce(generator <-chan string, out chan<- KeyValue, errChan chan<- error, cache *MappingCache, perf *PerfStats, mapper Mapper, reducer Reducer) {
	var wg sync.WaitGroup

	if cache != nil && cache.Data == nil {
		cache.Data = make(map[string]*cacheValues)
	}

	accumulator := make(chan KeyValue)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runMap(generator, accumulator, errChan, cache, perf, mapper)
		close(accumulator)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runReduce(accumulator, out, errChan, perf, reducer)
		close(out)
	}()

	wg.Wait()
}

// ReducePassThrough passes the values mapped directly as-is.
type ReducePassThrough struct {
}

// Reduce implements Reducer.
func (r *ReducePassThrough) Reduce(io ReduceIO) error {
	key := io.ReduceKey()
	for i := range io.ReduceValues() {
		io.Output(key, i)
	}
	return nil
}

// Private bits.

type cacheValues struct {
	dirty bool
	Items []serializedKeyValue
}

type serializedKeyValue struct {
	Key   string
	Value []byte // GobEncoded object.
}

type mapIO struct {
	mapKey       string
	mapperOutput chan<- KeyValue
	cache        *MappingCache
	errChan      chan<- error
}

func (m *mapIO) MapKey() string {
	return m.mapKey
}

func (m *mapIO) Emit(reduceKey string, reduceValue interface{}) {
	if m.cache != nil {
		t := reflect.TypeOf(reduceValue)
		if m.cache.valueType != t {
			m.errChan <- fmt.Errorf("expected type %v, got %v", m.cache.valueType, t)
		}
		m.cache.add(m.mapKey, reduceKey, reduceValue, m.errChan)
	}
	m.mapperOutput <- KeyValue{reduceKey, reduceValue}
}

type reduceIO struct {
	reduceKey     string
	reducerInput  chan interface{}
	reducerOutput chan<- KeyValue
}

func (r *reduceIO) ReduceKey() string {
	return r.reduceKey
}

func (r *reduceIO) ReduceValues() <-chan interface{} {
	return r.reducerInput
}

func (r *reduceIO) Output(finalKey string, finalValue interface{}) {
	r.reducerOutput <- KeyValue{finalKey, finalValue}
}

func runMap(generator <-chan string, accumulator chan<- KeyValue, errChan chan<- error, c *MappingCache, p *PerfStats, mapper Mapper) {
	var wg sync.WaitGroup
	for mapKey := range generator {
		wg.Add(1)
		if p != nil {
			atomic.AddInt64(&p.mappersRunning, 1)
		}
		go func(key string) {
			defer wg.Done()
			defer func() {
				if p != nil {
					atomic.AddInt64(&p.mappersRunning, -1)
				}
			}()
			if c != nil {
				if v := c.get(key, errChan); v != nil {
					// Cache hit.
					if p != nil {
						atomic.AddInt64(&p.cacheHits, 1)
					}
					for i := range v {
						accumulator <- i
					}
					return
				}
			}
			if p != nil {
				atomic.AddInt64(&p.cacheMisses, 1)
			}
			if err := mapper.Map(&mapIO{key, accumulator, c, errChan}); err != nil {
				errChan <- fmt.Errorf("failed to map %s: %s", key, err)
			}
		}(mapKey)
	}
	wg.Wait()

	if c != nil {
		for _, v := range c.Data {
			v.dirty = false
		}
	}
}

func runReduce(accumulator <-chan KeyValue, out chan<- KeyValue, errChan chan<- error, p *PerfStats, reducer Reducer) {
	var lock sync.Mutex
	buffer := make(map[string]*reduceIO)
	var wgReducers sync.WaitGroup
	var wgSeeds sync.WaitGroup

	// For each emitted key pair.
	for kp := range accumulator {
		lock.Lock()
		r, ok := buffer[kp.Key]
		lock.Unlock()

		if !ok {
			r = &reduceIO{
				reduceKey:     kp.Key,
				reducerInput:  make(chan interface{}),
				reducerOutput: out,
			}

			lock.Lock()
			buffer[kp.Key] = r
			lock.Unlock()

			// Start the reducer.
			wgReducers.Add(1)
			if p != nil {
				atomic.AddInt64(&p.reducersRunning, 1)
			}
			go func(io *reduceIO) {
				defer wgReducers.Done()
				defer func() {
					if p != nil {
						atomic.AddInt64(&p.reducersRunning, -1)
					}
				}()
				if err := reducer.Reduce(io); err != nil {
					errChan <- fmt.Errorf("failed to reduce %s: %s", io.reduceKey, err)
				}
			}(r)
		}

		// Push the value.
		wgSeeds.Add(1)
		go func(io *reduceIO, v interface{}) {
			defer wgSeeds.Done()
			io.reducerInput <- v
		}(r, kp.Value)
	}

	wgSeeds.Wait()
	for _, r := range buffer {
		close(r.reducerInput)
	}
	wgReducers.Wait()
}

func (c *MappingCache) get(key string, errChan chan<- error) <-chan KeyValue {
	c.lock.Lock()
	v, ok := c.Data[key]
	c.lock.Unlock()

	if !ok || v.dirty || v.Items == nil {
		return nil
	}
	out := make(chan KeyValue)
	go func() {
		for _, i := range v.Items {
			// Creates a pointer to valueType.
			obj := reflect.New(c.valueType)
			if err := gob.NewDecoder(bytes.NewBuffer(i.Value)).DecodeValue(obj); err == nil {
				// reflect.New() returns a *pointer* to type c.valueType, so deference
				// the pointer here.
				out <- KeyValue{i.Key, obj.Elem().Interface()}
			} else {
				errChan <- fmt.Errorf("failed to decode from cache for key %s: %s", key, err)
			}
		}
		close(out)
	}()
	return out
}

func (c *MappingCache) add(mapKey, reduceKey string, v interface{}, errChan chan<- error) {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		errChan <- fmt.Errorf("failed to encode to cache key %s: %s", mapKey, err)
		return
	}
	item := serializedKeyValue{reduceKey, buf.Bytes()}

	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Data[mapKey] == nil {
		c.Data[mapKey] = &cacheValues{Items: make([]serializedKeyValue, 0, 1)}
	}
	values := c.Data[mapKey]
	values.dirty = true
	values.Items = append(values.Items, item)
}
