package clog

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ValueClog struct {
	// For concurrency purposes; it's only the public API methods that should
	// call Clog.mu
	// The private ones should not.

	path        string
	initialized bool

	cl          *cleaner
	maxSegBytes uint64

	// The latest segment is at the end of list
	// ie; clog.segments[ len(clog.segments)-1 ] should give us the latest segment.
	segments []*segment
	// TODO: maybe the latest segment should be at index 0.
	// This would make append easier, see cleaner.go
}

func NewValueLog(path string, maxSegBytes uint64, maxLogBytes uint64, maxLogAge time.Duration) (ValueClog, error) {
	// maxSegBytes is a property of segment.
	//   It is size in bytes each segment can be, before been considered full & a new one created in its place.
	// maxLogBytes is a property of clog.
	//   It is size in bytes the log can allowed to be; once reached, some segments are deleted.
	// maxLogAge is a property of clog.
	//   It is age in seconds a log can be; once reached, some older segments are deleted.
	//
	sentinel := ValueClog{}

	c, err := newCleaner(maxLogBytes, maxLogAge)
	if err != nil {
		return sentinel, err
	}
	l := ValueClog{
		path:        path,
		cl:          c,
		initialized: true,
		maxSegBytes: maxSegBytes,
	}

	errA := l.createPath()
	if errA != nil {
		return sentinel, errA
	}

	newValueLog, errB := l.open()
	if errB != nil {
		return sentinel, errB
	}
	l = newValueLog

	return l, nil
}

func (l ValueClog) Path() string {
	return l.path
}

func (l ValueClog) createPath() error {
	err := os.MkdirAll(l.path, ownerReadableWritable)
	if err != nil {
		return errMkDir(err)
	}
	return nil
}

func (l ValueClog) open() (ValueClog, error) {
	newValueLog := l

	if !l.initialized {
		return newValueLog, errLogNotInitialized
	}

	files, err := os.ReadDir(l.path)
	if err != nil {
		return newValueLog, errReadDir(err)
	}

	segs := []*segment{}
	for _, file := range files {
		if filepath.Ext(file.Name()) == lFileSuffix {
			// files are given names that have the timestamp in utc before the suffix, see tNow()
			fNoExt := strings.TrimSuffix(file.Name(), lFileSuffix)
			n, errA := strconv.ParseUint(fNoExt, 10, 64)
			if errA != nil {
				return newValueLog, errParseToInt64(errA)
			}
			seg, errB := newSegment(l.path, n, l.maxSegBytes)
			if errB != nil {
				return newValueLog, errB
			}
			segs = append(segs, seg)
		}
	}

	if len(segs) == 0 {
		// the directory is empty. create a new file/segment
		t := tNow()
		seg, errC := newSegment(l.path, t, l.maxSegBytes)
		if errC != nil {
			return newValueLog, errC
		}
		newValueLog = l.segmentWrite([]*segment{seg}, nil)
	} else {
		// sort: the latest segment should be at the end of list
		sort.Slice(segs,
			func(i, j int) bool {
				return segs[i].baseOffset < segs[j].baseOffset
			},
		)
		newValueLog = l.segmentWrite(segs, nil)
	}

	segs = nil // gc
	return newValueLog, nil
}

func (l ValueClog) segmentWrite(segs []*segment, seg *segment) ValueClog {
	// all synchronizations should be in one method

	// In all methods of clog, this should be the only hit for
	// grep -irSn 'l.segments =' .
	// TODO: add that grep to CI and make sure it only returns 1 result

	newValueLog := l

	if seg != nil {
		segs = append(segs, seg)
	}
	newValueLog.segments = segs

	return newValueLog
}

func (l ValueClog) Append(b []byte) (ValueClog, error) {
	newValueLog := l
	if !l.initialized {
		return newValueLog, errLogNotInitialized
	}

	if l.toSplit() {
		newValueLog, err := l.split()
		if err != nil {
			return newValueLog, err
		}
	}

	a, errA := l.activeSegment()
	if errA != nil {
		return newValueLog, errA
	}
	return newValueLog, a.Append(b)
}

func (l ValueClog) toSplit() bool {
	a, err := l.activeSegment()
	if err != nil {
		// we have no active segment, we thus need to create one
		return true
	}
	return a.IsFull()
}

func (l ValueClog) activeSegment() (*segment, error) {
	_len := len(l.segmentRead())
	if _len <= 0 {
		return nil, errNoActiveSegment
	}
	return l.segmentRead()[_len-1], nil
}

func (l ValueClog) segmentRead() []*segment {
	// In all methods of clog, this should be the only hit for
	// grep -irSn '= l.segments' .
	// TODO: add that grep to CI and make sure it only returns 1 result
	segs := l.segments
	return segs
}

func (l ValueClog) split() (ValueClog, error) {
	newValueLog := l
	if !l.initialized {
		return newValueLog, errLogNotInitialized
	}

	// NB: we have to get the active segment before creating a new one.
	earlierActive, _ := l.activeSegment()
	// we do not care if l.activeSegment() has an error.
	// we just want the active segment before we split and form a new active seg.

	t := tNow()
	seg, errA := newSegment(l.path, t, l.maxSegBytes)
	if errA != nil {
		return newValueLog, errA
	}

	// TODO: do we need to maintain all the segments in a list or just the active one?
	// maybe we do for fast reads??
	newValueLog = l.segmentWrite(l.segmentRead(), seg)

	if earlierActive != nil {
		// we do not care about this error.
		// because the log now has a new active segment
		_ = earlierActive.close()
	}

	return newValueLog, nil
}
