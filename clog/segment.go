package clog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	errOpenFile             = func(err error) error { return fmt.Errorf("open file failed: %w", err) }
	errStatFile             = func(err error) error { return fmt.Errorf("stat file failed: %w", err) }
	errSegmentWrite         = func(err error) error { return fmt.Errorf("segment write failed: %w", err) }
	errPartialWriteTruncate = func(err error) error { return fmt.Errorf("partial write, truncate segment failed: %w", err) }
	errSegmentSync          = func(err error) error { return fmt.Errorf("segment sync failed: %w", err) }
	errSegmentClose         = func(err error) error { return fmt.Errorf("segment close failed: %w", err) }
	errSegmentRemove        = func(err error) error { return fmt.Errorf("segment remove failed: %w", err) }
	errSegmentRead          = func(err error) error { return fmt.Errorf("segment read failed: %w", err) }
)

type readWriteCloserSyncerTruncater interface {
	io.ReadWriteCloser
	Name() string
	Sync() error
	Truncate(size int64) error
}

type segment struct {
	baseOffset uint64
	filePath   string

	// mu protects currentSegBytes, maxSegBytes, f & age
	mu              sync.RWMutex
	currentSegBytes uint64
	maxSegBytes     uint64
	f               readWriteCloserSyncerTruncater
	age             uint64 // diff between now() - baseOffset

	closed bool
}

func newSegment(path string, baseOffset uint64, maxSegBytes uint64) (*segment, error) {
	filePath := filepath.Join(path, fmt.Sprintf("%d.log", baseOffset))
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, ownerReadableWritable)
	if err != nil {
		return nil, errOpenFile(err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, errStatFile(err)
	}

	var age uint64
	now := tNow()
	if baseOffset > now {
		// The segment appears to have been created in the future. Is that you Einstein?
		// Although it would be pleasing to Albert, we are not amused.
		// Set age to 0; as if the segment has just been created.
		//
		// uint64(7) - uint64(12) == 18446744073709551611
		// because of overflow. So we have to prevent that
		age = uint64(0)
	} else {
		age = now - baseOffset
	}

	return &segment{
		filePath:        filePath,
		baseOffset:      baseOffset,
		currentSegBytes: uint64(fi.Size()),
		maxSegBytes:     maxSegBytes,
		f:               f,
		age:             age,
	}, nil
}

func (s *segment) String() string {
	return fmt.Sprintf("segment{file: %s, baseOffset:%d}", s.filePath, s.baseOffset)
}

// IsFull shows whether the segment holds as much data as it is allowed to
func (s *segment) IsFull() bool {
	s.mu.RLock()
	r := s.currentSegBytes >= s.maxSegBytes
	s.mu.RUnlock()
	return r
}

// Append adds an item to the segment.
// To append more items at once use AppendBulk
func (s *segment) Append(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO: write in encoded form
	// https://github.com/komuw/shifta/issues/1
	n, err := s.f.Write(b)
	if err != nil {
		return errSegmentWrite(err)
	}

	if n != len(b) {
		// partial write.
		errA := s.f.Truncate(int64(s.currentSegBytes))
		if errA != nil {
			return errPartialWriteTruncate(errA)
		}
	} else {
		s.currentSegBytes = s.currentSegBytes + uint64(n)
		s.age = tNow() - s.baseOffset
	}

	errB := s.f.Sync()
	if errB != nil {
		return errSegmentSync(errB)
	}

	return nil
}

// AppendBulk adds multiple items to the segment.
// To append one item at a time use Append
func (s *segment) AppendBulk(bbs [][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return errors.New("TODO: implement appendBulk")
}

// Delete removes a segment from the filesystem.
func (s *segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f == nil {
		return nil
	}

	err := s.close()
	if err != nil {
		return err
	}
	errA := os.Remove(s.filePath)
	if errA != nil {
		return errSegmentRemove(errA)
	}

	// do we need to do this?
	s.f = nil
	s = nil

	return nil
}

func (s *segment) close() error {
	if s.closed {
		return nil
	}

	// Note: sync of file does not also sync its directory.
	//  TODO: sync the directory also
	err := s.f.Sync()
	if err != nil {
		return errSegmentSync(err)
	}

	errA := s.f.Close()
	if errA != nil {
		return errSegmentClose(errA)
	}

	s.closed = true
	return nil
}

// Read reads all data from the segment.
func (s *segment) Read() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// TODO: we should not read the whole file to memory.
	b, err := os.ReadFile(s.f.Name())
	if err != nil {
		return nil, errSegmentRead(err)
	}

	return b, nil
}
