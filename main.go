package main

import (
	"strings"
	"sync"

	"github.com/komuw/shifta/clog"
)

func main() {
	wg := sync.WaitGroup{}

	l, e := clog.New("/tmp/clog/orders", 100, 5, 7)
	if e != nil {
		panic(e)
	}
	// defer os.RemoveAll(l.path)

	// 1. Append
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 6; j++ {
				msg := "someMessage"
				errA := l.Append([]byte(strings.Repeat(msg, j*900)))
				if errA != nil {
					panic(errA)
				}
			}
		}()
	}

	// 2. Read
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 6; j++ {
				_, _, errB := l.Read(0)
				if errB != nil {
					panic(errB)
				}
			}
		}()
	}

	// 3. Clean
	for i := 0; i < 800; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errC := l.Clean()
			if errC != nil {
				panic(errC)
			}
		}()
	}

	// 4. Append
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 23; j++ {
				msg := "HeyString"
				errD := l.Append([]byte(strings.Repeat(msg, j*231)))
				if errD != nil {
					panic(errD)
				}
			}
		}()
	}

	// 5. Clean
	for i := 0; i < 19; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errE := l.Clean()
			if errE != nil {
				panic(errE)
			}
		}()
	}

	// 6. Read
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 6; j++ {
				_, _, errF := l.Read(3)
				if errF != nil {
					panic(errF)
				}
			}
		}()
	}

	wg.Wait()
}
