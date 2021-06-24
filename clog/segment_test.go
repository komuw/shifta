package clog

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"
)

// a mock readWriteCloserSyncerTruncater that fails in various configurable ways
type mockFileFail struct {
	fName       string
	errWrite    error
	errTruncate error
	shortWrite  bool
}

func (m mockFileFail) Name() string                     { return m.fName }
func (m mockFileFail) Read(p []byte) (n int, err error) { return 1, nil }
func (m mockFileFail) Write(p []byte) (n int, err error) {
	if m.shortWrite {
		return (n / 2), m.errWrite
	}
	return n, m.errWrite
}
func (m mockFileFail) Close() error              { return nil }
func (m mockFileFail) Sync() error               { return nil }
func (m mockFileFail) Truncate(size int64) error { return m.errTruncate }

func TestNewSegment(t *testing.T) {
	// https://github.com/golang/go/wiki/TableDrivenTests#parallel-testing
	t.Parallel()

	t.Run("with normal baseOffset", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		path, err := ioutil.TempDir("/tmp", "clog")
		c.Assert(err, qt.IsNil)
		defer os.RemoveAll(path)

		baseOffset := tNow()
		s, err := newSegment(path, baseOffset, 100)
		c.Assert(err, qt.IsNil)

		c.Assert(s.baseOffset, qt.Equals, baseOffset)
		c.Assert(s.currentSegBytes, qt.Equals, uint64(0))
		c.Assert(s.maxSegBytes, qt.Equals, uint64(100))
		c.Assert(s.closed, qt.Equals, false)
		c.Assert(s.age > 0, qt.IsTrue)
	})

	t.Run("with zero baseOffset", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		path, err := ioutil.TempDir("/tmp", "clog")
		c.Assert(err, qt.IsNil)
		defer os.RemoveAll(path)

		bo := uint64(0)
		s, errSeg := newSegment(path, bo, 100)
		c.Assert(errSeg, qt.IsNil)

		c.Assert(s.baseOffset, qt.Equals, bo)
		c.Assert(s.currentSegBytes, qt.Equals, uint64(0))
		c.Assert(s.maxSegBytes, qt.Equals, uint64(100))
		c.Assert(s.age > 0, qt.IsTrue)
	})

	t.Run("with baseOffset far in the future", func(t *testing.T) {
		t.Parallel()
		c := qt.New(t)

		path, err := ioutil.TempDir("/tmp", "clog")
		c.Assert(err, qt.IsNil)
		defer os.RemoveAll(path)

		baseOffset := tNow() * 7
		s, err := newSegment(path, baseOffset, 100)
		c.Assert(err, qt.IsNil)

		c.Assert(s.baseOffset, qt.Equals, baseOffset)
		c.Assert(s.currentSegBytes, qt.Equals, uint64(0))
		c.Assert(s.maxSegBytes, qt.Equals, uint64(100))
		c.Assert(s.age, qt.Equals, uint64(0))
	})
}

func TestIsFull(t *testing.T) {
	t.Parallel()

	path, err := ioutil.TempDir("/tmp", "clog")
	if err != nil {
		t.Fatal("\n\t", err)
	}
	defer os.RemoveAll(path)

	baseOffset := tNow()
	s, err := newSegment(path, baseOffset, 100)
	if err != nil {
		t.Fatal("\n\t", err)
	}

	if s.IsFull() != false {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.IsFull(), false)
	}
	if s.currentSegBytes != 0 {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.currentSegBytes, 0)
	}

	count := s.maxSegBytes * 2
	msg := []byte(strings.Repeat("a", int(count)))
	errA := s.Append(msg)
	if errA != nil {
		t.Fatal("\n\t", errA)
	}

	if s.IsFull() != true {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.IsFull(), true)
	}
	if s.currentSegBytes != count {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.currentSegBytes, count)
	}
}

func createSegmentForTests(t *testing.T) (*segment, func()) {
	path, err := ioutil.TempDir("/tmp", "clog")
	if err != nil {
		t.Fatal("\n\t", err)
	}

	baseOffset := tNow()
	s, errA := newSegment(path, baseOffset, 100)
	if errA != nil {
		t.Fatal("\n\t", errA)
	}
	if s.currentSegBytes != 0 {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.currentSegBytes, 0)
	}
	if s.maxSegBytes != 100 {
		t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.maxSegBytes, 100)
	}

	return s, func() { os.RemoveAll(path) }
}

func TestSegmentAppend(t *testing.T) {
	t.Parallel()

	t.Run("writes go to disk", func(t *testing.T) {
		t.Parallel()

		s, removePath := createSegmentForTests(t)
		defer removePath()

		msg := []byte("hello world")
		errA := s.Append(msg)
		if errA != nil {
			t.Fatal("\n\t", errA)
		}

		f, errB := os.Open(s.filePath)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		defer f.Close()
		rMsg, errC := io.ReadAll(f)
		if errC != nil {
			t.Fatal("\n\t", errC)
		}
		if len(rMsg) != len(msg) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(rMsg), len(msg))
		}

		if !cmp.Equal(rMsg, msg) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", string(rMsg), string(msg))
		}
	})

	t.Run("writes are additive", func(t *testing.T) {
		t.Parallel()

		s, removePath := createSegmentForTests(t)
		defer removePath()

		msg1 := []byte("hello world")
		msg2 := msg1
		msg3 := []byte("123456")
		err := s.Append(msg1)
		if err != nil {
			t.Fatal("\n\t", err)
		}
		errAppend1 := s.Append(msg2)
		if errAppend1 != nil {
			t.Fatal("\n\t", errAppend1)
		}
		errAppend2 := s.Append(msg3)
		if errAppend2 != nil {
			t.Fatal("\n\t", errAppend2)
		}

		f, errB := os.Open(s.filePath)
		if errB != nil {
			t.Fatal("\n\t", errB)
		}
		defer f.Close()
		rMsg, errC := io.ReadAll(f)
		if errC != nil {
			t.Fatal("\n\t", errC)
		}
		if len(rMsg) != (len(msg1) + len(msg2) + len(msg3)) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", len(rMsg), (len(msg1) + len(msg2) + len(msg3)))
		}

		hold := [][]byte{}
		hold = append(hold, msg1)
		hold = append(hold, msg2)
		hold = append(hold, msg3)
		res := bytes.Join(hold, []byte(""))
		if !cmp.Equal(rMsg, res) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", string(rMsg), string(res))
		}
	})

	t.Run("s.f.Write() failing", func(t *testing.T) {
		t.Parallel()

		s, removePath := createSegmentForTests(t)
		defer removePath()
		setErr := errors.New("writing to `mockFileFailWrites` failed")
		s.f = mockFileFail{errWrite: setErr, fName: s.f.Name()}

		msg := []byte("hello world")
		err := s.Append(msg)
		if !errors.Is(err, setErr) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", err, setErr)
		}
		if err == nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", nil, setErr)
		}
	})

	t.Run("short writes", func(t *testing.T) {
		t.Parallel()

		s, removePath := createSegmentForTests(t)
		defer removePath()
		errTruncate := errors.New("truncating to `mockFileFailWrites` failed")

		s.f = mockFileFail{shortWrite: true, errTruncate: errTruncate, fName: s.f.Name()}
		msg := []byte("hello world")
		err := s.Append(msg)
		if !errors.Is(err, errTruncate) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", err, errTruncate)
		}
		if err == nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", nil, errTruncate)
		}
	})
}

func TestClose(t *testing.T) {
	t.Parallel()

	t.Run("close", func(t *testing.T) {
		t.Parallel()

		s, removePath := createSegmentForTests(t)
		defer removePath()

		if s.f == nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.f, "not nil")
		}

		err := s.close()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		if s.closed != true {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.closed, true)
		}
	})
}

func TestDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		s, removePath := createSegmentForTests(t)
		defer removePath()

		if s.f == nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.f, "not nil")
		}

		err := s.Delete()
		if err != nil {
			t.Fatal("\n\t", err)
		}

		// delete should set s.f to nil
		if s.f != nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", s.f, nil)
		}

		f, errA := os.Open(s.filePath)
		var pathError *os.PathError
		if !errors.As(errA, &pathError) {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", errA, os.PathError{})
		}
		if f != nil {
			t.Errorf("\ngot \n\t%#+v \nwanted \n\t%#+v", f, nil)
		}
	})
}
