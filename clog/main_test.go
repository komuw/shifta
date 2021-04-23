package clog

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"
)

const acceptableCodeCoverage = 0.6 // 60%

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags

	exitCode := m.Run()
	if exitCode == 0 && testing.CoverMode() != "" {
		coverage := testing.Coverage()
		// note: for some reason the value of `coverage` is always less
		// than the one reported on the terminal by go test
		if coverage < acceptableCodeCoverage {
			fmt.Printf("\n\tThe test code coverage has fallen below the acceptable value of %v. The current value is %v. \n", acceptableCodeCoverage, coverage)
			exitCode = -1
		}
	}

	exitCode = leakDetector(exitCode)
	os.Exit(exitCode)
}

// see:
// https://github.com/uber-go/goleak/blob/v1.1.10/testmain.go#L40-L52
func leakDetector(exitCode int) int {
	if exitCode == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			exitCode = 1
		}
	}
	return exitCode
}
