package clog_test

import (
	"fmt"
	"os"
	"time"

	"github.com/komuw/shifta/clog"
)

func ExampleClog_Append() {
	l, e := clog.New(
		"/tmp/customerOrders",
		80_000_000,     /*80Mb*/
		1_000_000_000,  /*1Gb*/
		3*24*time.Hour, /*3days*/
	)
	if e != nil {
		panic(e)
	}
	defer os.RemoveAll(l.Path())

	err := l.Append([]byte("customer #1 ordered 3 shoes."))
	if err != nil {
		panic(err)
	}

	// Unordered output:
}

func ExampleClog_Read() {
	l, e := clog.New(
		"/tmp/customerOrders",
		80_000_000,     /*80Mb*/
		1_000_000_000,  /*1Gb*/
		3*24*time.Hour, /*3days*/
	)
	if e != nil {
		panic(e)
	}
	defer os.RemoveAll(l.Path())

	err := l.Append([]byte("Nasir bin Olu Dara Jones ordered 3 shoes."))
	if err != nil {
		panic(err)
	}

	dataRead, _, err := l.Read(0, 0)
	fmt.Print(string(dataRead))

	// Unordered output:
	// Nasir bin Olu Dara Jones ordered 3 shoes.
}
