package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	outFileName      = "current-data"
	bufSize          = 8192
	segmentThreshold = 3
)

type hashIndex map[string]int64

type IndexOp struct {
	isWrite bool
	key     string
	index   int64
}

type PutOp struct {
	entry entry
	resp  chan error
}

type KeyPosition struct {
	segment  *Segment
	position int64
}

type ReadOp struct {
	key     string
	resp    chan string
	errResp chan error
}

type SegmentManager struct {
	segments         []*Segment
	lastSegmentIndex int
}

type Db struct {
	out            *os.File
	outPath        string
	outOffset      int64
	dir            string
	segmentSize    int64
	indexOps       chan IndexOp
	keyPositions   chan *KeyPosition
	putOps         chan PutOp
	putDone        chan error
	readOps        chan ReadOp
	segmentManager *SegmentManager
	fileMutex      sync.Mutex
	indexMutex     sync.Mutex
}

type Segment struct {
	index    hashIndex
	filePath string
}

var (
	ErrNotFound = fmt.Errorf("record does not exist")
)

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segmentManager: &SegmentManager{
			segments: make([]*Segment, 0),
		},
		dir:          dir,
		segmentSize:  segmentSize,
		indexOps:     make(chan IndexOp),
		keyPositions: make(chan *KeyPosition),
		putOps:       make(chan PutOp),
		putDone:      make(chan error),
		readOps:      make(chan ReadOp, 10), // Обмеження кількості паралельних операцій читання
	}

	if err := db.createSegment(); err != nil {
		return nil, err
	}

	if err := db.recoverAll(); err != nil && err != io.EOF {
		return nil, err
	}

	db.startIndexRoutine()
	db.startPutRoutine()
	db.startReadWorkers(5)

	return db, nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) startIndexRoutine() {
	go func() {
		for op := range db.indexOps {
			db.indexMutex.Lock()
			if op.isWrite {
				db.setKey(op.key, op.index)
			} else {
				s, p, err := db.getSegmentAndPos(op.key)
				if err != nil {
					db.keyPositions <- nil
				} else {
					db.keyPositions <- &KeyPosition{s, p}
				}
			}
			db.indexMutex.Unlock()
		}
	}()
}

func (db *Db) startPutRoutine() {
	go func() {
		for {
			op := <-db.putOps
			db.fileMutex.Lock()
			length := op.entry.GetLength()
			stat, err := db.out.Stat()
			if err != nil {
				op.resp <- err
				db.fileMutex.Unlock()
				continue
			}
			if stat.Size()+length > db.segmentSize {
				err := db.createSegment()
				if err != nil {
					op.resp <- err
					db.fileMutex.Unlock()
					continue
				}
			}
			n, err := db.out.Write(op.entry.Encode())
			if err == nil {
				db.indexOps <- IndexOp{
					isWrite: true,
					key:     op.entry.key,
					index:   int64(n),
				}
			}
			op.resp <- nil
			db.fileMutex.Unlock()
		}
	}()
}

func (db *Db) startReadWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go func() {
			for op := range db.readOps {
				keyPos := db.getPos(op.key)
				if keyPos == nil {
					op.errResp <- ErrNotFound
				} else {
					value, err := keyPos.segment.getFromSegment(keyPos.position)
					if err != nil {
						op.errResp <- err
					} else {
						op.resp <- value
					}
				}
			}
		}()
	}
}

func (db *Db) createSegment() error {
	filePath := db.generateNewFileName()
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	newSegment := &Segment{
		filePath: filePath,
		index:    make(hashIndex),
	}

	db.out = f
	db.outOffset = 0
	db.outPath = filePath
	db.segmentManager.segments = append(db.segmentManager.segments, newSegment)
	if len(db.segmentManager.segments) >= segmentThreshold {
		db.performOldSegmentsCompaction()
	}

	return nil
}

func (db *Db) generateNewFileName() string {
	result := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.segmentManager.lastSegmentIndex))
	db.segmentManager.lastSegmentIndex++
	return result
}

func (db *Db) performOldSegmentsCompaction() {
	go func() {
		filePath := db.generateNewFileName()
		newSegment := &Segment{
			filePath: filePath,
			index:    make(hashIndex),
		}
		var offset int64
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return
		}
		lastSegmentIndex := len(db.segmentManager.segments) - 2
		for i := 0; i <= lastSegmentIndex; i++ {
			s := db.segmentManager.segments[i]
			for key, index := range s.index {
				if i < lastSegmentIndex {
					isInNewerSegments := findKeyInSegments(db.segmentManager.segments[i+1:lastSegmentIndex+1], key)
					if isInNewerSegments {
						continue
					}
				}
				value, _ := s.getFromSegment(index)
				e := entry{
					key:   key,
					value: value,
				}
				n, err := f.Write(e.Encode())
				if err == nil {
					newSegment.index[key] = offset
					offset += int64(n)
				}
			}
		}
		db.segmentManager.segments = []*Segment{newSegment, db.getLastSegment()}
	}()
}

func findKeyInSegments(segments []*Segment, key string) bool {
	for _, s := range segments {
		if _, ok := s.index[key]; ok {
			return true
		}
	}
	return false
}

func (db *Db) recoverAll() error {
	for _, segment := range db.segmentManager.segments {
		if err := db.recoverSegment(segment); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) recoverSegment(segment *Segment) error {
	f, err := os.Open(segment.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := db.recover(f); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (db *Db) recover(f *os.File) error {
	var err error
	var buf [bufSize]byte

	in := bufio.NewReaderSize(f, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.setKey(e.key, int64(n))
		}
	}
	return err
}

func (db *Db) setKey(key string, n int64) {
	db.getLastSegment().index[key] = db.outOffset
	db.outOffset += n
}

func (db *Db) getSegmentAndPos(key string) (*Segment, int64, error) {
	for i := range db.segmentManager.segments {
		s := db.segmentManager.segments[len(db.segmentManager.segments)-i-1]
		pos, ok := s.index[key]
		if ok {
			return s, pos, nil
		}
	}

	return nil, 0, ErrNotFound
}

func (db *Db) getPos(key string) *KeyPosition {
	op := IndexOp{
		isWrite: false,
		key:     key,
	}
	db.indexOps <- op
	return <-db.keyPositions
}

func (db *Db) Get(key string) (string, error) {
	resp := make(chan string)
	errResp := make(chan error)
	db.readOps <- ReadOp{
		key:     key,
		resp:    resp,
		errResp: errResp,
	}

	select {
	case value := <-resp:
		return value, nil
	case err := <-errResp:
		return "", err
	}
}

func (db *Db) Put(key, value string) error {
	resp := make(chan error)
	db.putOps <- PutOp{
		entry: entry{
			key:   key,
			value: value,
		},
		resp: resp,
	}
	err := <-resp
	close(resp)
	return err
}

func (db *Db) getLastSegment() *Segment {
	return db.segmentManager.segments[len(db.segmentManager.segments)-1]
}

func (s *Segment) getFromSegment(position int64) (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}
