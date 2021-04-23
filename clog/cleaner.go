package clog

import (
	"errors"
	"time"
)

var errBadCleaner = errors.New("cleaner cannot have negative or zero maxLogBytes/maxLogAge ")

type cleaner struct {
	maxLogBytes uint64
	maxLogAge   time.Duration
}

func newCleaner(maxLogBytes uint64, maxLogAge time.Duration) (*cleaner, error) {
	if maxLogBytes <= 0 || maxLogAge <= 0 {
		return nil, errBadCleaner
	}

	// the kafka defaults are:
	// - maxLogBytes is 1GB
	// - maxLogAge 7days
	return &cleaner{maxLogBytes: maxLogBytes, maxLogAge: maxLogAge}, nil
}

func (c *cleaner) clean(segs []*segment) ([]*segment, error) {
	if len(segs) <= 1 {
		// retain at least one
		return segs, nil
	}

	//  limit by number of bytes first.
	segs, err := c.cleanByBytes(segs)
	if err != nil {
		return nil, err
	}

	// by age.
	segs, errA := c.cleanByAge(segs)
	if errA != nil {
		return nil, errA
	}

	// TODO: check that the latest segment should be at the end of list
	return segs, nil
}

func (c *cleaner) cleanByBytes(segs []*segment) ([]*segment, error) {
	if len(segs) <= 1 {
		// retain at least one
		return segs, nil
	}

	var total uint64
	cleanedSegs := []*segment{}
	var indexOfCleanedSeg []int

	// start with most active segment
	for i := len(segs) - 1; i >= 0; i-- {
		s := segs[i]
		if total < c.maxLogBytes {
			// it means the first will always be added
			// we want the latest segment to always be at end of list. so we prepend instead of append
			cleanedSegs = append([]*segment{s}, cleanedSegs...)
			indexOfCleanedSeg = append(indexOfCleanedSeg, i)
		}
		s.mu.RLock()
		total = total + s.currentSegBytes
		s.mu.RUnlock()
	}

	if len(cleanedSegs) > 0 {
		for i := len(segs) - 1; i >= 0; i-- {
			if contains(indexOfCleanedSeg, i) {
				continue
			}
			s := segs[i]
			err := s.Delete()
			if err != nil {
				return segs, err
			}
		}
		return cleanedSegs, nil
	}
	return segs, nil
}

func (c *cleaner) cleanByAge(segs []*segment) ([]*segment, error) {
	if len(segs) <= 1 {
		return segs, nil
	}

	var total uint64
	cleanedSegs := []*segment{}
	var indexOfCleanedSeg []int

	// start with most active segment
	for i := len(segs) - 1; i >= 0; i-- {
		s := segs[i]
		if total < uint64(c.maxLogAge.Nanoseconds()) {
			// it means the first will always be added
			cleanedSegs = append([]*segment{s}, cleanedSegs...)
			indexOfCleanedSeg = append(indexOfCleanedSeg, i)
		}
		s.mu.RLock()
		total = total + s.age
		s.mu.RUnlock()
	}

	if len(cleanedSegs) > 0 {
		for i := len(segs) - 1; i >= 0; i-- {
			if contains(indexOfCleanedSeg, i) {
				continue
			}
			s := segs[i]
			err := s.Delete()
			if err != nil {
				return segs, err
			}
		}
		return cleanedSegs, nil
	}

	return segs, nil
}

// contains tells whether a contains x.
func contains(a []int, x int) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
