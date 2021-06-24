package clog

import (
	"os"
	"testing"
	"time"
)

/////////////////////////////// benches ////////////////////
/*
  go test -cpuprofile cpu.out -memprofile mem.out -benchmem -run=^$ -bench ^ github.com/komuw/shifta/clog

  1.
  BenchmarkValueShifta-8       1	9_734_399_086 ns/op	   53_936 B/op	    1052 allocs/op
  BenchmarkPointeryShifta-8    1	9_994_791_939 ns/op	   37_472 B/op	    1045 allocs/op

  2.
  BenchmarkValueShifta-8       1	9_902_466_933 ns/op	   54_112 B/op	    1051 allocs/op
  BenchmarkPointeryShifta-8    1	9_845_575_525 ns/op	   38_000 B/op	    1049 allocs/op

  3.
  BenchmarkValueShifta-8       1	9_925_726_552 ns/op	   54_120 B/op	    1052 allocs/op
  BenchmarkPointeryShifta-8    1	9_931_024_048 ns/op	   37_472 B/op	    1045 allocs/op

  go tool pprof cpu.out

    sum%        cum   cum%
	77.27%      200ms 45.45%  github.com/komuw/shifta/clog.BenchmarkPointeryShifta
	77.27%      200ms 45.45%  github.com/komuw/shifta/clog.PointeryShifta
	77.27%      190ms 43.18%  github.com/komuw/shifta/clog.(*Clog).Append
	77.27%      180ms 40.91%  github.com/komuw/shifta/clog.BenchmarkValueShifta
	77.27%      180ms 40.91%  github.com/komuw/shifta/clog.ValueClog.Append
	77.27%      180ms 40.91%  github.com/komuw/shifta/clog.ValueShifta
*/

func PointeryShifta() int {
	p := "/tmp/customerOrders/PointeryShifta"
	l, e := New(
		p,
		80_000_000,     /*80Mb*/
		1_000_000_000,  /*1Gb*/
		3*24*time.Hour, /*3days*/
	)
	if e != nil {
		panic(e)
	}
	defer os.RemoveAll(l.Path())

	res := 0
	for i := 0; i < 1_000; i++ {
		err := l.Append([]byte("customer #1 ordered 3 shoes."))
		if err != nil {
			panic(err)
		}
		res = i
	}

	return res
}

var result int

func ValueShifta() int {
	p := "/tmp/customerOrders/ValueShifta"
	l, e := NewValueLog(
		p,
		80_000_000,     /*80Mb*/
		1_000_000_000,  /*1Gb*/
		3*24*time.Hour, /*3days*/
	)
	if e != nil {
		panic(e)
	}
	defer os.RemoveAll(l.Path())

	res := 0
	for i := 0; i < 1_000; i++ {
		m, err := l.Append([]byte("customer #1 ordered 3 shoes."))
		if err != nil {
			panic(err)
		}
		l = m
		res = i
	}

	return res
}

func BenchmarkValueShifta(b *testing.B) {
	var r int
	for n := 0; n < b.N; n++ {
		// always record the result of Fib to prevent
		// the compiler eliminating the function call.
		r = ValueShifta()
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = r
}

func BenchmarkPointeryShifta(b *testing.B) {
	var r int
	for n := 0; n < b.N; n++ {
		// always record the result of Fib to prevent
		// the compiler eliminating the function call.
		r = PointeryShifta()
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = r
}
