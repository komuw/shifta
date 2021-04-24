package clog

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func createPathForTests(t *testing.T) (string, func()) {
	path, err := ioutil.TempDir("/tmp", "Clog")
	if err != nil {
		t.Fatal("\n\t", err)
	}

	return path, func() { os.RemoveAll(path) }
}

type createLogConfig struct {
	maxSegBytes uint64
	maxLogBytes uint64
	maxLogAge   time.Duration
}

func createClogForTests(t *testing.T, conf ...createLogConfig) (*Clog, func()) {
	path, removePath := createPathForTests(t)

	maxSegBytes := uint64(100)
	maxLogBytes := uint64(1)
	maxLogAge := time.Duration(1)
	if conf != nil {
		maxSegBytes = conf[0].maxSegBytes
		maxLogBytes = conf[0].maxLogBytes
		maxLogAge = conf[0].maxLogAge
	}
	l, e := New(path, maxSegBytes, maxLogBytes, maxLogAge)
	if e != nil {
		t.Fatal("\n\t", e)
	}

	return l, removePath
}

func TestCreatePath(t *testing.T) {
	// https://github.com/golang/go/wiki/TableDrivenTests#parallel-testing
	t.Parallel()

	path := "/tmp/TestClogTestCreatePath"
	defer os.RemoveAll(path)

	l := &Clog{path: path}

	err := l.createPath()
	if err != nil {
		t.Fatal("\n\t", err)
	}
}

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("open with no existing log files", func(t *testing.T) {
		t.Parallel()

		cl, errI := newCleaner(100, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}
		path, removePath := createPathForTests(t)
		l := &Clog{path: path, initialized: true, cl: cl}
		defer removePath()

		err := l.open()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		segs := l.segmentRead()
		if len(segs) != 1 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), 1)
		}
	})

	t.Run("open with existing log files", func(t *testing.T) {
		t.Parallel()

		cl, errI := newCleaner(100, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}
		path, removePath := createPathForTests(t)
		l := &Clog{path: path, initialized: true, cl: cl}
		defer removePath()

		segs := l.segmentRead()
		if len(segs) != 0 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), 0)
		}

		// create other log files in l.path directory
		for i := 100; i < 109; i++ {
			_, err := os.Create(filepath.Join(l.path, fmt.Sprintf("%d.log", i)))
			if err != nil {
				t.Fatal("\n\t", err)
			}
		}

		err := l.open()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		segs2 := l.segmentRead()
		if len(segs2) != 9 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs2), 9)
		}
	})

	t.Run("existing log files have right size", func(t *testing.T) {
		t.Parallel()
		cl, errI := newCleaner(100, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}
		path, removePath := createPathForTests(t)
		l := &Clog{path: path, initialized: true, cl: cl}
		defer removePath()

		segs := l.segmentRead()
		if len(segs) != 0 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), 0)
		}

		// create other log files in l.path directory
		// and write to them
		msg := []byte("Hope springs eternal in the human breast.")
		for i := 100; i < 109; i++ {
			f, err := os.Create(filepath.Join(l.path, fmt.Sprintf("%d.log", i)))
			if err != nil {
				t.Fatal("\n\t", err)
			}
			n, err := f.Write(msg)
			if err != nil {
				t.Fatal("\n\t", err)
			}
			if len(msg) != n {
				t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(msg), n)
			}
		}

		err := l.open()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		segs2 := l.segmentRead()
		if len(segs2) != 9 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs2), 9)
		}

		for _, v := range segs2 {
			if v.currentSegBytes != uint64(len(msg)) {
				t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", v.currentSegBytes, len(msg))
			}
		}
	})

	t.Run("mis-named log files are rejected", func(t *testing.T) {
		t.Parallel()

		cl, errI := newCleaner(100, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}
		path, removePath := createPathForTests(t)
		l := &Clog{path: path, initialized: true, cl: cl}
		defer removePath()

		segs := l.segmentRead()
		if len(segs) != 0 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), 0)
		}

		// create log file sin l.path directory
		for i := 1; i < 5; i++ {
			_, err := os.Create(filepath.Join(l.path, fmt.Sprintf("%s-%d.log", "Malema", i)))
			if err != nil {
				t.Fatal("\n\t", err)
			}
		}

		errA := l.open()
		if errA == nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", errA, "nonNilError")
		}

		segs2 := l.segmentRead()
		if len(segs2) != 0 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs2), 0)
		}
	})

	t.Run("log files are sorted by offset", func(t *testing.T) {
		t.Parallel()

		cl, errI := newCleaner(100, 1)
		if errI != nil {
			t.Fatal("\n\t", errI)
		}
		path, removePath := createPathForTests(t)
		l := &Clog{path: path, initialized: true, cl: cl}
		defer removePath()

		segs := l.segmentRead()
		if len(segs) != 0 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), 0)
		}

		// create other log files in l.path directory
		// and write to them
		msg := []byte("Hope springs eternal in the human breast.")
		for i := 100; i < 109; i++ {
			f, err := os.Create(filepath.Join(l.path, fmt.Sprintf("%d.log", i)))
			if err != nil {
				t.Fatal("\n\t", err)
			}
			n, err := f.Write(msg)
			if err != nil {
				t.Fatal("\n\t", err)
			}
			if len(msg) != n {
				t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(msg), n)
			}
		}

		_, errCreate1 := os.Create(filepath.Join(l.path, fmt.Sprintf("%d.log", 1)))
		if errCreate1 != nil {
			t.Fatal("\n\t", errCreate1)
		}
		_, errCreate2 := os.Create(filepath.Join(l.path, fmt.Sprintf("%d.log", 3)))
		if errCreate2 != nil {
			t.Fatal("\n\t", errCreate2)
		}
		_, errCreate3 := os.Create(filepath.Join(l.path, fmt.Sprintf("%d.log", 88998)))
		if errCreate3 != nil {
			t.Fatal("\n\t", errCreate3)
		}

		errA := l.open()
		if errA != nil {
			t.Fatal("\n\t", errA)
		}

		segs2 := l.segmentRead()
		if len(segs2) != 12 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs2), 12)
		}

		if segs2[0].baseOffset != 1 {
			t.Error("\n segments are not sorted.\n")
		}
		if segs2[1].baseOffset != 3 {
			t.Error("\n segments are not sorted.\n")
		}
		if segs2[5].baseOffset != 103 {
			t.Error("\n segments are not sorted.\n")
		}
		if segs2[11].baseOffset != 88998 {
			t.Error("\n segments are not sorted.\n")
		}

		a, err := l.activeSegment()
		if err != nil {
			t.Fatal("\n\t", err)
		}
		if a.baseOffset != 88998 {
			t.Error("\n segments are not sorted.\n")
		}
	})
}

func TestActiveSegment(t *testing.T) {
	t.Parallel()

	l, removePath := createClogForTests(t)
	defer removePath()

	a, err := l.activeSegment()
	if err != nil {
		t.Fatal("\n\t", err)
	}
	if a == nil {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", a, "notNil")
	}

	errA := l.open()
	if errA != nil {
		t.Fatal("\n\t", errA)
	}

	a, errB := l.activeSegment()
	if errB != nil {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", errB, nil)
	}
	if a == nil {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", a, "notNil")
	}
}

func TestLogAppend(t *testing.T) {
	t.Parallel()

	t.Run("append before log initialization", func(t *testing.T) {
		t.Parallel()

		path, removePath := createPathForTests(t)
		l := &Clog{path: path}
		defer removePath()

		msg := []byte("hello")
		err := l.Append(msg)
		if !errors.Is(err, errLogNotInitialized) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", err, errLogNotInitialized)
		}
		if err == nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", err, "nil")
		}
	})

	t.Run("append before opening log", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		msg := []byte("hello")
		err := l.Append(msg)
		if err != nil {
			t.Fatal("\n\t", err)
		}
	})

	t.Run("append with NO split", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		errA := l.open()
		if errA != nil {
			t.Fatal("\n\t", errA)
		}

		if l.toSplit() == true {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", l.toSplit(), false)
		}

		msg := []byte("hello")
		errB := l.Append(msg)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
	})

	t.Run("append with split", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		errA := l.open()
		if errA != nil {
			t.Fatal("\n\t", errA)
		}

		{
			// add messages greater than segment.maxBytes
			s, errB := l.activeSegment()
			if errB != nil {
				t.Fatal("\n\t", errB)
			}

			if l.toSplit() == true {
				t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", l.toSplit(), false)
			}

			count := s.maxSegBytes * 2
			msg := []byte(strings.Repeat("a", int(count)))
			errC := l.Append(msg)
			if errC != nil {
				t.Fatal("\n\t", errC)
			}
		}

		// the next append should require a split
		if l.toSplit() == false {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", l.toSplit(), true)
		}

		msg := []byte("hello")
		errD := l.Append(msg)
		if errD != nil {
			t.Fatal("\n\t", errD)
		}
	})
}

func TestLogSplit(t *testing.T) {
	t.Parallel()

	t.Run("split before opening log", func(t *testing.T) {
		t.Parallel()

		path, removePath := createPathForTests(t)
		l := &Clog{path: path}
		defer removePath()

		if l.toSplit() == false {
			// if we have no segments at all(like before opening a log file)
			// l.toSplit() should be true
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", l.toSplit(), true)
		}

		err := l.split()
		if !errors.Is(err, errLogNotInitialized) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", err, errLogNotInitialized)
		}
	})

	t.Run("split on non-full segment log", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		err := l.open()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		if l.toSplit() == true {
			// segment is not full
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", l.toSplit(), false)
		}

		errA := l.split()
		if err != nil {
			t.Fatal("\n\t", errA)
		}

		segs := l.segmentRead()
		if len(segs) != 2 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), 2)
		}
	})

	t.Run("on split, new segment is made the activeSegment", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		err := l.open()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		if l.toSplit() == true {
			// segment is not full
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", l.toSplit(), false)
		}

		errA := l.split()
		if err != nil {
			t.Fatal("\n\t", errA)
		}

		segs := l.segmentRead()
		if len(segs) != 2 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(segs), 2)
		}

		s, errB := l.activeSegment()
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		if s.baseOffset < l.segments[0].baseOffset {
			t.Errorf("\n new split segment was not made the activeSegment")
		}
	})

	t.Run("split when current segment is full, creates a new segment", func(t *testing.T) {
		t.Parallel()

		maxSegmentBytes := uint64(78)
		path, removePath := createPathForTests(t)
		defer removePath()
		l, e := New(path, maxSegmentBytes, 1, 1)
		if e != nil {
			t.Fatal("\n\t", e)
		}

		if l.toSplit() == true {
			// segment is not full yet
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", l.toSplit(), false)
		}

		// append a little
		msg := []byte("hello")
		err := l.Append(msg)
		if err != nil {
			t.Fatal("\n\t", err)
		}
		if len(l.segments) != 1 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 1)
		}

		// append more than s.maxSegBytes
		count := maxSegmentBytes * 4
		msg = []byte(strings.Repeat("a", int(count)))
		errA := l.Append(msg)
		if errA != nil {
			t.Fatal("\n\t", errA)
		}
		if len(l.segments) != 1 {
			// the first append that has more than s.maxSegBytes does not cause a split
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 1)
		}

		// append a little. This should cause a segment split
		msg = []byte("hello")
		errB := l.Append(msg)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		if len(l.segments) != 2 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 2)
		}

		// the activeSegment should be the newly added one
		s, errC := l.activeSegment()
		if errC != nil {
			t.Fatal("\n\t", errC)
		}
		if s.baseOffset < l.segments[0].baseOffset {
			t.Errorf("\n new split segment was not made the activeSegment")
		}
	})
}

func TestLogClean(t *testing.T) {
	t.Parallel()

	t.Run("able to clean", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		l.cl.maxLogBytes = 700
		defer removePath()

		s, err := l.activeSegment()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		count := s.maxSegBytes * 4
		msg := []byte(strings.Repeat("a", int(count)))
		errA := l.Append(msg)
		if errA != nil {
			t.Fatal("\n\t", errA)
		}

		for i := 0; i < 10; i++ {
			// create more segments
			errB := l.split()
			if errB != nil {
				t.Fatal("\n\t", errB)
			}

			errC := l.Append(msg)
			if errC != nil {
				t.Fatal("\n\t", errC)
			}
		}
		if len(l.segments) != 11 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 11)
		}

		errE := l.Clean()
		if errE != nil {
			t.Fatal("\n\t", errE)
		}
		if len(l.segments) != 1 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 1)
		}

		errF := l.Append(msg)
		if errF != nil {
			t.Fatal("\n\t", errF)
		}
	})
}

func TestLogRead(t *testing.T) {
	t.Parallel()

	t.Run("read from offset 0 for log with one segment", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		oneMsg := strings.Repeat("a", int(l.maxSegBytes*7))
		msg := []byte(oneMsg)
		errA := l.Append(msg)
		if errA != nil {
			t.Fatal("\n\t", errA)
		}

		blob, lastReadOffset, errB := l.Read(0)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		if len(l.segments) != 1 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 1)
		}
		if lastReadOffset != l.segments[0].baseOffset {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", lastReadOffset, l.segments[0].baseOffset)
		}
		if string(blob) != oneMsg {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", string(blob), oneMsg)
		}
	})

	t.Run("read from offset 0 for log with many segments", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		msg := []byte(strings.Repeat("a", int(l.maxSegBytes*7)))
		for i := 0; i < 23; i++ {
			// append msgs that are larger than l.maxSegBytes
			// this will cause creation of more segments
			errA := l.Append(msg)
			if errA != nil {
				t.Fatal("\n\t", errA)
			}
		}
		if len(l.segments) != 23 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 23)
		}

		blob, lastReadOffset, errB := l.Read(0)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		if lastReadOffset != l.segments[22].baseOffset {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", lastReadOffset, l.segments[22].baseOffset)
		}
		if len(blob) != 16100 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(blob), 16100)
		}
		if !cmp.Equal(blob[0], blob[22]) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", string(blob[0]), string(blob[22]))
		}
		if !cmp.Equal(blob[0], msg[0]) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", blob[0], msg[0])
		}
	})

	t.Run("read from a specific offset for log with many segments", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		msg := []byte(strings.Repeat("a", int(l.maxSegBytes*7)))
		for i := 0; i < 23; i++ {
			// append msgs that are larger than l.maxSegBytes
			// this will cause creation of more segments
			errA := l.Append(msg)
			if errA != nil {
				t.Fatal("\n\t", errA)
			}
		}
		if len(l.segments) != 23 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(l.segments), 23)
		}

		offset := l.segments[13].baseOffset + 3 // start from a number greater than the 13th segment's offset.
		blob, lastReadOffset, errB := l.Read(offset)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		if lastReadOffset != l.segments[22].baseOffset {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", lastReadOffset, l.segments[22].baseOffset)
		}
		if len(blob) != 6300 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(blob), 6300)
		}
		if !cmp.Equal(blob[0], blob[8]) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", string(blob[0]), string(blob[8]))
		}
		if !cmp.Equal(blob[0], msg[0]) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", blob[0], msg[0])
		}

		b, lo, errC := l.Read(lastReadOffset)
		if errC != nil {
			t.Fatal("\n\t", errC)
		}
		if len(b) != 0 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(b), 0)
		}
		if lo != 0 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", lo, 0)
		}
	})

	t.Run("read from a commitlog with data larger than maxToRead", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		targetMemBytes := internalMaxToRead * 4
		numSegs := 8
		memPerSeg := targetMemBytes / numSegs

		msg := []byte(strings.Repeat("a", memPerSeg))
		for i := 0; i <= numSegs; i++ {
			errA := l.Append(msg)
			if errA != nil {
				t.Fatal("\n\t", errA)
			}
		}

		// try and read the 4*maxToRead worth of data.
		blob, _, errB := l.Read(0)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		if len(blob) < internalMaxToRead {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(blob), internalMaxToRead)
		}
		if len(blob) > internalMaxToRead*2 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(blob), internalMaxToRead)
		}
	})

	t.Run("read from a commitlog where each segment is larger than the maxToRead const in l.Read", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t)
		defer removePath()

		msg := []byte(strings.Repeat("a", internalMaxToRead*2))
		for i := 0; i < 4; i++ {
			errA := l.Append(msg)
			if errA != nil {
				t.Fatal("\n\t", errA)
			}
		}

		blob, _, errB := l.Read(0)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		if len(blob) != internalMaxToRead*2 {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(blob), internalMaxToRead*3)
		}
	})
}

func TestCommitLogRaceDetection(t *testing.T) {
	t.Parallel()

	t.Run("test race conditions", func(t *testing.T) {
		t.Parallel()

		l, removePath := createClogForTests(t, createLogConfig{
			maxSegBytes: uint64(100),
			maxLogBytes: uint64(5),
			maxLogAge:   time.Duration(7),
		})
		defer removePath()
		wg := sync.WaitGroup{}

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
	})
}
