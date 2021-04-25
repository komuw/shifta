package clog

import (
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestCleaner(t *testing.T) {
	t.Parallel()

	t.Run("less bytes errors", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		_, errA := newCleaner(0, 1)
		c.Assert(errA, qt.ErrorMatches, errBadCleaner.Error())

		_, errB := newCleaner(uint64(0), 1)
		c.Assert(errB, qt.ErrorMatches, errBadCleaner.Error())
	})

	t.Run("less Age errors", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		_, errA := newCleaner(1, 0)
		c.Assert(errA, qt.ErrorMatches, errBadCleaner.Error())

		_, errB := newCleaner(1, -78)
		c.Assert(errB, qt.ErrorMatches, errBadCleaner.Error())
	})
}

func TestCleanByBytes(t *testing.T) {
	t.Parallel()

	t.Run("total log size is equal to cleaner.maxLogBytes", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 10
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			segs = append(segs, s)

			// each segment will store 1byte.
			// so size of all segments == maxLogBytes
			msg := []byte("a")
			err := s.Append(msg)
			c.Assert(err, qt.IsNil)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByBytes(segs)
		c.Assert(errB, qt.IsNil)

		// no cleaning should occur if size of the log == maxLogBytes
		c.Assert(cleanedSegs, qt.HasLen, totalSegments)
	})

	t.Run("total log size is less than cleaner.maxLogBytes", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 5
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			segs = append(segs, s)

			msg := []byte("a")
			err := s.Append(msg)
			c.Assert(err, qt.IsNil)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByBytes(segs)
		c.Assert(errB, qt.IsNil)
		// no cleaning should occur
		c.Assert(cleanedSegs, qt.HasLen, totalSegments)
	})

	t.Run("total log size is greater than cleaner.maxLogBytes", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 20
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			segs = append(segs, s)

			msg := []byte("a")
			err := s.Append(msg)
			c.Assert(err, qt.IsNil)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByBytes(segs)
		c.Assert(errB, qt.IsNil)
		// cleaning SHOULD occur
		c.Assert(cleanedSegs, qt.HasLen, totalSegments/2)
	})

	t.Run("latest/active segment should be preserved", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 20
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			s.baseOffset = uint64(i)
			segs = append(segs, s)

			msg := []byte(strings.Repeat("a", 4))
			err := s.Append(msg)
			c.Assert(err, qt.IsNil)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByBytes(segs)
		c.Assert(errB, qt.IsNil)
		// cleaning SHOULD occur
		c.Assert(cleanedSegs, qt.HasLen, 3)

		c.Assert(cleanedSegs[0].baseOffset, qt.Equals, uint64(17))
		c.Assert(cleanedSegs[1].baseOffset, qt.Equals, uint64(18))
		c.Assert(cleanedSegs[2].baseOffset, qt.Equals, uint64(19))

		activeSegment := func(segs []*segment) *segment {
			// see Clog.activeSegment()
			_len := len(segs)
			return segs[_len-1]
		}
		activeSeg := activeSegment(cleanedSegs)
		c.Assert(activeSeg.baseOffset, qt.Equals, uint64(19))
	})

	t.Run("atleast one segment should be preserved", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 20
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			s.baseOffset = uint64(i)
			segs = append(segs, s)

			// each segment on its own is greater than maxLogBytes
			msg := []byte(strings.Repeat("a", int(maxLogBytes*3)))
			err := s.Append(msg)
			c.Assert(err, qt.IsNil)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByBytes(segs)
		c.Assert(errB, qt.IsNil)
		// one segment should be left AND it should be the latest one
		c.Assert(cleanedSegs, qt.HasLen, 1)
		c.Assert(cleanedSegs[0].baseOffset, qt.Equals, uint64(19))
	})
}

func TestCleanByAge(t *testing.T) {
	t.Parallel()

	t.Run("total log Age is equal to cleaner.maxLogAge", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogAge := time.Duration(100)
		cl, errI := newCleaner(1, maxLogAge)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 10
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			// each seg is 10durations old.
			// thus totalAge of log == maxLogAge
			s.age = 10
			segs = append(segs, s)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByAge(segs)
		c.Assert(errB, qt.IsNil)
		// no cleaning should occur if Age of the log == maxLogAge
		c.Assert(cleanedSegs, qt.HasLen, totalSegments)
	})

	t.Run("total log Age is less than cleaner.maxLogAge", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		// fix when https://github.com/dgryski/semgrep-go/issues/29
		// gets fixed.
		maxLogAge := time.Duration(10000)
		cl, errI := newCleaner(1, maxLogAge)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 10
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			// each seg is 10durations old.
			// thus totalAge of log < maxLogAge
			s.age = 10
			segs = append(segs, s)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByAge(segs)
		c.Assert(errB, qt.IsNil)
		// no cleaning should occur
		c.Assert(cleanedSegs, qt.HasLen, totalSegments)
	})

	t.Run("total log Age is greater than cleaner.maxLogAge", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogAge := time.Duration(13)
		cl, errI := newCleaner(1, maxLogAge)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 100
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			// each seg is 10durations old.
			// thus totalAge of log > maxLogAge
			s.age = 10
			segs = append(segs, s)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByAge(segs)
		c.Assert(errB, qt.IsNil)
		// cleaning should occur
		c.Assert(cleanedSegs, qt.HasLen, 2)
	})

	t.Run("latest/active segment should be preserved", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		maxLogAge := time.Duration(35)
		cl, errI := newCleaner(1, maxLogAge)
		c.Assert(errI, qt.IsNil)

		segs := []*segment{}
		totalSegments := 24
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			// each seg is 10durations old.
			// thus totalAge of log > maxLogAge
			s.age = 10
			s.baseOffset = uint64(i)
			segs = append(segs, s)
		}
		c.Assert(segs, qt.HasLen, totalSegments)

		cleanedSegs, errB := cl.cleanByAge(segs)
		c.Assert(errB, qt.IsNil)
		// cleaning should occur
		c.Assert(cleanedSegs, qt.HasLen, 4)

		c.Assert(cleanedSegs[0].baseOffset, qt.Equals, uint64(20))
		c.Assert(cleanedSegs[1].baseOffset, qt.Equals, uint64(21))
		c.Assert(cleanedSegs[2].baseOffset, qt.Equals, uint64(22))
		c.Assert(cleanedSegs[3].baseOffset, qt.Equals, uint64(23))
	})

}
