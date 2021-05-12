package main

import (
	"fmt"
	"runtime"
	"time"
)

func main() {
	requestData(1)
	time.Sleep(time.Second * 7)
	fmt.Printf("Number of hanging goroutines: %d", runtime.NumGoroutine())
}

func requestData(timeout time.Duration) string {
	dataChan := make(chan string, 1)

	go func() {
		newData := requestFromSlowServer()
		dataChan <- newData // block
	}()

	select {
	case result := <-dataChan:
		fmt.Printf("[+] request returned: %s", result)
		return result
	case <-time.After(timeout):
		fmt.Println("[!] request timeout!")
		return ""
	}
}

func requestFromSlowServer() string {
	// time.Sleep(time.Second * 1)
	return "very important data"
}
