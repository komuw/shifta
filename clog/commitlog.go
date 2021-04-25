// Package clog provides an implementation of a commitLog.
//
// A commitlog is a sequence of records, where each new record is appended to the log.
// It is represented as a directory in a filesystem that contains one or more files that are called segments.
// It is the segments that actually hold data.
//
package clog

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO:
// Use the following to code review for possible race conditions
// or for places that will require synchronizations.
//
// regex to match struct field reads;
// '= \w+\.\w+'
// regex to match struct field writes;
// '\w+\.\w+ = '
// We should add this to the to CI.
// grep -irSnE '\w+\.\w+ = ' /path/to/repo | wc -l
// That should output number of hits. We should assert that number is what we expect.

const (
	lFileSuffix = ".log"
)

// owner can read, write, & execute
// group can only read
// others have no permissions
var ownerReadableWritable fs.FileMode = 0o740

var (
	errNoActiveSegment   = errors.New("commitLog has no active segment")
	errLogNotInitialized = errors.New("commitLog has not been initialized. use New method")
	errMkDir             = func(err error) error { return fmt.Errorf("mkdir failed: %w", err) }
	errReadDir           = func(err error) error { return fmt.Errorf("read dir failed: %w", err) }
	errParseToInt64      = func(err error) error { return fmt.Errorf("parse file to uint64 failed: %w", err) }
)

// tNow returns the number of nanoseconds elapsed since January 1, 1970 UTC.
// The result is undefined for a date before the year 1678 or after 2262
// See time.UnixNano() for more
func tNow() uint64 {
	return uint64(time.Now().In(time.UTC).UnixNano())
}

// Clog is a commitLog.
//
// To create a commitlog, use the New method.
type Clog struct {
	path        string
	initialized bool

	cl          *cleaner
	maxSegBytes uint64

	// mu protects the []segment slice
	// whenever a method of clog needs to read from clog.segments take a mu.RLock
	// whenever a method of clog needs to write to clog.segments take a mu.Lock
	mu sync.RWMutex
	// The latest segment is at the end of list
	// ie; clog.segments[ len(clog.segments)-1 ] should give us the latest segment.
	segments []*segment
	// TODO: maybe the latest segment should be at index 0.
	// This would make append easier, see cleaner.go
}

// New creates a commitLog.
//
// The commitlog will be created in the filesystem at path.
// Each segment will hold upto maxSegBytes of content, the value of maxSegBytes should be significantly smaller than RAM.
// Once a commitlog gets larger than maxLogBytes, some segments gets deleted from the filesystem.
// Likewise, once a commitlog gets older than maxLogAge, some segments gets deleted from the filesystem.
// When creating a commitlog, you should choose values of maxSegBytes, maxLogBytes & maxLogAge
// that are appropriate for your usecase.
// For comparison purposes, the Kafka default values for maxLogBytes & maxLogAge is 1GB and 7days respectively.
//
// usage:
//   l, errN := New("/tmp/orders", 100, 5, 7)
//   errA := l.Append([]byte("order # 1"))
//
func New(path string, maxSegBytes uint64, maxLogBytes uint64, maxLogAge time.Duration) (*Clog, error) {
	// maxSegBytes is a property of segment.
	//   It is size in bytes each segment can be, before been considered full & a new one created in its place.
	// maxLogBytes is a property of clog.
	//   It is size in bytes the log can allowed to be; once reached, some segments are deleted.
	// maxLogAge is a property of clog.
	//   It is age in seconds a log can be; once reached, some older segments are deleted.
	//
	c, err := newCleaner(maxLogBytes, maxLogAge)
	if err != nil {
		return nil, err
	}
	l := &Clog{
		path:        path,
		cl:          c,
		initialized: true,
		maxSegBytes: maxSegBytes,
	}

	errA := l.createPath()
	if errA != nil {
		return nil, errA
	}

	errB := l.open()
	if errB != nil {
		return nil, errB
	}

	return l, nil
}

func (l *Clog) String() string {
	return fmt.Sprintf("clog{path:%s, segments: %s}", l.path, l.segments)
}

func (l *Clog) createPath() error {
	err := os.MkdirAll(l.path, ownerReadableWritable)
	if err != nil {
		return errMkDir(err)
	}
	return nil
}

func (l *Clog) open() error {
	if !l.initialized {
		return errLogNotInitialized
	}

	files, err := os.ReadDir(l.path)
	if err != nil {
		return errReadDir(err)
	}

	segs := []*segment{}
	for _, file := range files {
		if filepath.Ext(file.Name()) == lFileSuffix {
			// files are given names that have the timestamp in utc before the suffix, see tNow()
			fNoExt := strings.TrimSuffix(file.Name(), lFileSuffix)
			n, errA := strconv.ParseUint(fNoExt, 10, 64)
			if errA != nil {
				return errParseToInt64(errA)
			}
			seg, errB := newSegment(l.path, n, l.maxSegBytes)
			if errB != nil {
				return errB
			}
			segs = append(segs, seg)
		}
	}

	if len(segs) == 0 {
		// the directory is empty. create a new file/segment
		t := tNow()
		seg, errC := newSegment(l.path, t, l.maxSegBytes)
		if errC != nil {
			return errC
		}
		l.segmentWrite([]*segment{seg}, nil)
	} else {
		// sort: the latest segment should be at the end of list
		sort.Slice(segs,
			func(i, j int) bool {
				return segs[i].baseOffset < segs[j].baseOffset
			},
		)
		l.segmentWrite(segs, nil)
	}

	segs = nil // gc
	return nil
}

func (l *Clog) segmentWrite(segs []*segment, seg *segment) {
	// all synchronizations should be in one method

	// In all methods of clog, this should be the only hit for
	// grep -irSn 'l.segments =' .
	// TODO: add that grep to CI and make sure it only returns 1 result
	if seg != nil {
		segs = append(segs, seg)
	}
	l.segments = segs
}

func (l *Clog) segmentRead() []*segment {
	// In all methods of clog, this should be the only hit for
	// grep -irSn '= l.segments' .
	// TODO: add that grep to CI and make sure it only returns 1 result
	segs := l.segments
	return segs
}

func (l *Clog) activeSegment() (*segment, error) {
	_len := len(l.segmentRead())
	if _len <= 0 {
		return nil, errNoActiveSegment
	}
	return l.segmentRead()[_len-1], nil
}

// Path returns the directory, in the filesystem, of the commitlog.
func (l *Clog) Path() string {
	return l.path
}

// Append adds an item to the commitLog.
// To append more items at once use AppendBulk
func (l *Clog) Append(b []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.initialized {
		return errLogNotInitialized
	}

	if l.toSplit() {
		err := l.split()
		if err != nil {
			return err
		}
	}

	a, errA := l.activeSegment()
	if errA != nil {
		return errA
	}
	return a.Append(b)
}

// AppendBulk adds multiple items to the commitLog.
// To append one item at a time use Append
func (l *Clog) AppendBulk(bbs [][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.initialized {
		return errLogNotInitialized
	}
	return errors.New("TODO: implement appendBulk")
}

func (l *Clog) toSplit() bool {
	a, err := l.activeSegment()
	if err != nil {
		// we have no active segment, we thus need to create one
		return true
	}
	return a.IsFull()
}

func (l *Clog) split() error {
	if !l.initialized {
		return errLogNotInitialized
	}

	// NB: we have to get the active segment before creating a new one.
	earlierActive, _ := l.activeSegment()
	// we do not care if l.activeSegment() has an error.
	// we just want the active segment before we split and form a new active seg.

	t := tNow()
	seg, errA := newSegment(l.path, t, l.maxSegBytes)
	if errA != nil {
		return errA
	}

	// TODO: do we need to maintain all the segments in a list or just the active one?
	// maybe we do for fast reads??
	l.segmentWrite(l.segmentRead(), seg)

	if earlierActive != nil {
		// we do not care about this error.
		// because the log now has a new active segment
		_ = earlierActive.close()
	}
	return nil
}

// Clean deletes segments that are;
// (a) larger than maxLogBytes
// (b) older than maxLogAge
// from the commitlog(and filesystem)
func (l *Clog) Clean() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	cleaned, err := l.cl.clean(l.segments)
	if err != nil {
		return err
	}
	l.segments = cleaned

	return nil
}

const internalMaxToRead = (64 * 1000 * 1000) // 64Mb

// Read reads upto maxToRead bytes from the commitlog starting at offset(exclusive).
// maxToRead is a hint, this method can read more or less than that.
// If maxToRead == 0 then a default value will be chosen.
//
// If it encounters an error, it will still return all the data read so far,
// its offset and an error.
func (l *Clog) Read(offset uint64, maxToRead uint64) (dataRead []byte, lastReadOffset uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var max int = int(maxToRead)
	if max <= 0 {
		max = internalMaxToRead
	} else if max > (internalMaxToRead * 10) {
		// prevent a case where a malicious actor sends
		// a maxToRead that is >>> computer RAM leading to OOM.
		max = internalMaxToRead * 10
	}

	var sizeReadSofar int
	for _, seg := range l.segments {
		if seg.baseOffset > offset {
			// We exclude the offset from reads.
			// This allows people to use lastReadOffset in subsequent calls to l.Read
			b, errR := seg.Read()
			if errR != nil {
				// TODO: should we return based on one error?
				return dataRead, lastReadOffset, errR
				// TODO: test that if error occurs, we still return whatever has been read so far.
			}
			dataRead = append(dataRead, b...)
			lastReadOffset = seg.baseOffset
			sizeReadSofar = sizeReadSofar + len(b)

			if sizeReadSofar >= max {
				break
			}
		}
	}

	// clog reads the whole data from a segment, even if the individual segment
	// has data greater than maxToRead.
	// Thus, the returned lastReadOffset is safe to be used in subsequent l.Read calls
	// since the segment it belongs to wont be read again.
	return dataRead, lastReadOffset, nil
}
