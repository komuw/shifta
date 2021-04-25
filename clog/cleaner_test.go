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

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

		segs := []*segment{}
		totalSegments := 5
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			segs = append(segs, s)

			msg := []byte("a")
			err := s.Append(msg)
			if err != nil {
				t.Fatal("\n\t", err)
			}
		}
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByBytes(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// no cleaning should occur
		if len(cleanedSegs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), totalSegments)
		}
	})

	t.Run("total log size is greater than cleaner.maxLogBytes", func(t *testing.T) {
		t.Parallel()

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

		segs := []*segment{}
		totalSegments := 20
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			segs = append(segs, s)

			msg := []byte("a")
			err := s.Append(msg)
			if err != nil {
				t.Fatal("\n\t", err)
			}
		}
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByBytes(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// cleaning SHOULD occur
		if len(cleanedSegs) != totalSegments/2 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), totalSegments/2)
		}
	})

	t.Run("latest/active segment should be preserved", func(t *testing.T) {
		t.Parallel()

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

		segs := []*segment{}
		totalSegments := 20
		for i := 0; i < totalSegments; i++ {
			s, removePath := createSegmentForTests(t)
			defer removePath()
			s.baseOffset = uint64(i)
			segs = append(segs, s)

			msg := []byte(strings.Repeat("a", 4))
			err := s.Append(msg)
			if err != nil {
				t.Fatal("\n\t", err)
			}
		}
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByBytes(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// cleaning SHOULD occur
		if len(cleanedSegs) != 3 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), totalSegments)
		}

		if cleanedSegs[0].baseOffset != 17 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[0].baseOffset, 17)
		}
		if cleanedSegs[1].baseOffset != 18 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[1].baseOffset, 18)
		}
		if cleanedSegs[2].baseOffset != 19 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[2].baseOffset, 19)
		}

		activeSegment := func(segs []*segment) *segment {
			// see Clog.activeSegment()
			_len := len(segs)
			return segs[_len-1]
		}
		activeSeg := activeSegment(cleanedSegs)
		if activeSeg.baseOffset != 19 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", activeSeg.baseOffset, 19)
		}
	})

	t.Run("atleast one segment should be preserved", func(t *testing.T) {
		t.Parallel()

		maxLogBytes := uint64(10)
		cl, errI := newCleaner(maxLogBytes, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

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
			if err != nil {
				t.Fatal("\n\t", err)
			}
		}
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByBytes(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// one segment should be left AND it should be the latest one
		if len(cleanedSegs) != 1 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), 1)
		}
		if cleanedSegs[0].baseOffset != 19 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[0].baseOffset, 19)
		}
	})
}

func TestCleanByAge(t *testing.T) {
	t.Parallel()

	t.Run("total log Age is equal to cleaner.maxLogAge", func(t *testing.T) {
		t.Parallel()

		maxLogAge := time.Duration(100)
		cl, errI := newCleaner(1, maxLogAge)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

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
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByAge(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// no cleaning should occur if Age of the log == maxLogAge
		if len(cleanedSegs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), totalSegments)
		}
	})

	t.Run("total log Age is less than cleaner.maxLogAge", func(t *testing.T) {
		t.Parallel()

		// fix when https://github.com/dgryski/semgrep-go/issues/29
		// gets fixed.
		maxLogAge := time.Duration(10000)
		cl, errI := newCleaner(1, maxLogAge)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

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
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByAge(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// no cleaning should occur
		if len(cleanedSegs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), totalSegments)
		}
	})

	t.Run("total log Age is greater than cleaner.maxLogAge", func(t *testing.T) {
		t.Parallel()

		maxLogAge := time.Duration(13)
		cl, errI := newCleaner(1, maxLogAge)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

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
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByAge(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// cleaning should occur
		if len(cleanedSegs) != 2 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), 2)
		}
	})

	t.Run("latest/active segment should be preserved", func(t *testing.T) {
		t.Parallel()

		maxLogAge := time.Duration(35)
		cl, errI := newCleaner(1, maxLogAge)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}

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
		if len(segs) != totalSegments {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), totalSegments)
		}

		cleanedSegs, errB := cl.cleanByAge(segs)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		// cleaning should occur
		if len(cleanedSegs) != 4 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(cleanedSegs), 4)
		}

		if cleanedSegs[0].baseOffset != 20 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[0].baseOffset, 20)
		}
		if cleanedSegs[1].baseOffset != 21 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[1].baseOffset, 21)
		}
		if cleanedSegs[2].baseOffset != 22 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[2].baseOffset, 22)
		}
		if cleanedSegs[3].baseOffset != 23 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", cleanedSegs[3].baseOffset, 23)
		}
	})
}
