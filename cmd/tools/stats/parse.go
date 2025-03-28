package main

import (
	"fmt"
	"os"
	"sort"
	"syscall"
	"unsafe"
)

const (
	RECORD_SIZE = 16 // 8+8 bytes
)

type Segment struct {
	FilePath string
	MmapData []byte
	Pairs    []Int64Pair
}

type Int64Pair struct {
	PK int64
	TS int64
}

type QueryResult struct {
	SegmentID string
	TSList    []int64
}

func LoadSegment(path string) (*Segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if fi.Size()%RECORD_SIZE != 0 {
		return nil, fmt.Errorf("invalid file size")
	}

	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	pairCount := len(mmap) / RECORD_SIZE
	pairs := unsafe.Slice((*Int64Pair)(unsafe.Pointer(&mmap[0])), pairCount)

	if !sort.SliceIsSorted(pairs, func(i, j int) bool {
		return pairs[i].PK < pairs[j].PK
	}) {
		syscall.Munmap(mmap)
		return nil, fmt.Errorf("segment file not sorted")
	}

	return &Segment{
		FilePath: path,
		MmapData: mmap,
		Pairs:    pairs,
	}, nil
}

func (s *Segment) Close() {
	if s.MmapData != nil {
		syscall.Munmap(s.MmapData)
		s.MmapData = nil
		s.Pairs = nil
	}
}

func (s *Segment) Query(pk int64) ([]int64, bool) {
	n := len(s.Pairs)
	i := sort.Search(n, func(i int) bool { return s.Pairs[i].PK >= pk })
	if i >= n || s.Pairs[i].PK != pk {
		return nil, false
	}

	j := i
	for j < n && s.Pairs[j].PK == pk {
		j++
	}

	results := make([]int64, 0, j-i)
	for k := i; k < j; k++ {
		results = append(results, s.Pairs[k].TS)
	}
	return results, true
}

type QueryEngine struct {
	segments []*Segment
}

func NewQueryEngine(segmentPaths []string) (*QueryEngine, error) {
	engine := &QueryEngine{}
	for _, path := range segmentPaths {
		seg, err := LoadSegment(path)
		if err != nil {
			engine.Close()
			return nil, fmt.Errorf("load %s failed: %v", path, err)
		}
		engine.segments = append(engine.segments, seg)
	}
	return engine, nil
}

func (e *QueryEngine) Close() {
	for _, seg := range e.segments {
		seg.Close()
	}
	e.segments = nil
}

func (e *QueryEngine) BatchQuery(pks []int64) map[int64][]QueryResult {
	result := make(map[int64][]QueryResult, len(pks))

	for _, pk := range pks {
		var pkResults []QueryResult
		for _, seg := range e.segments {
			if ts, found := seg.Query(pk); found {
				pkResults = append(pkResults, QueryResult{
					SegmentID: seg.FilePath,
					TSList:    ts,
				})
			}
		}
		if len(pkResults) > 0 {
			result[pk] = pkResults
		}
	}

	return result
}
