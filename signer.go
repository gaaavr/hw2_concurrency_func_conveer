package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for data := range in {
		wg.Add(1)

		go func(data interface{}) {
			dataInt, ok := data.(int)
			if !ok {
				panic("data type is not int")
			}

			dataStr := strconv.Itoa(dataInt)
			var dataCrc32, dataCrc32WithMd string

			wg2 := &sync.WaitGroup{}
			wg2.Add(2)

			go func() {
				dataCrc32 = DataSignerCrc32(dataStr)
				wg2.Done()
			}()

			go func() {
				mu.Lock()
				md := DataSignerMd5(dataStr)
				mu.Unlock()
				dataCrc32WithMd = DataSignerCrc32(md)
				wg2.Done()
			}()

			wg2.Wait()

			out <- dataCrc32 + "~" + dataCrc32WithMd
			wg.Done()
		}(data)
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		wg.Add(1)
		go func(data interface{}) {
			dataStr, ok := data.(string)
			if !ok {
				panic("data type is not string")
			}

			wg2 := &sync.WaitGroup{}

			var result [6]string

			for i := 0; i < 6; i++ {
				wg2.Add(1)

				go func(i int) {
					result[i] = DataSignerCrc32(strconv.Itoa(i) + dataStr)
					wg2.Done()
				}(i)
			}

			wg2.Wait()

			out <- strings.Join(result[:], "")
			wg.Done()
		}(data)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	result := make([]string, 0, cap(in))
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for data := range in {
		wg.Add(1)
		go func(data interface{}) {
			dataStr, ok := data.(string)
			if !ok {
				panic("data type is not string")
			}

			mu.Lock()
			result = append(result, dataStr)
			mu.Unlock()

			wg.Done()
		}(data)

	}

	wg.Wait()

	sort.Strings(result)

	out <- strings.Join(result, "_")
}

func ExecutePipeline(jobs ...job) {
	inCh := make(chan interface{}, MaxInputDataLen)
	outCh := make(chan interface{}, MaxInputDataLen)

	wg := &sync.WaitGroup{}

	for _, j := range jobs {
		wg.Add(1)
		go func(inCh, outCh chan interface{}, j job) {
			j(inCh, outCh)
			close(outCh)
			wg.Done()
		}(inCh, outCh, j)

		inCh = outCh
		outCh = make(chan interface{}, MaxInputDataLen)
	}

	wg.Wait()
}
