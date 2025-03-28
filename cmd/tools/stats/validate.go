package main

import (
	"fmt"
	"github.com/milvus-io/milvus/internal/storage"
	"path/filepath"
)

func validatePKAndBF(chunkManager storage.ChunkManager, fileDir string) {
	fmt.Println("start to validate ", fileDir)

	pkFile, err := extractPKFileFromDir(fileDir)
	if err != nil {
		panic(err)
	}
	if len(pkFile) == 0 {
		fmt.Println("empty PK file")
		return
	}
	segment, err := LoadSegment(pkFile[0])
	if err != nil {
		panic(err)
	}

	bfs, err := LoadStatsFromDir(chunkManager, fileDir)
	if err != nil {
		panic(err)
	}
	found := false
	for _, pair := range segment.Pairs {
		//fmt.Println("len of bfs", len(bfs))
		for _, bf := range bfs {
			if !bf.PkExist(storage.NewInt64PrimaryKey(pair.PK)) {
				continue
				//fmt.Println(fmt.Sprintf("%s find %d not in bf", fileDir, pair.PK))
				//found_exception = true
				//break
			} else {
				found = true
			}
		}
		if !found {
			fmt.Println(fmt.Sprintf("%s find %d not in bf", fileDir, pair.PK))
		}
	}
}

// 3499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf

func queryPKs(chunkManager storage.ChunkManager, pks []int64, fileDirs []string) {
	fileNames := make([]string, 0, len(pks))
	for _, fileDir := range fileDirs {
		pkFile, err := extractPKFileFromDir(fileDir)
		if err != nil || len(pkFile) == 0 {
			continue
		}
		fileNames = append(fileNames, pkFile[0])
	}
	engine, err := NewQueryEngine(fileNames)
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	results := engine.BatchQuery(pks)

	for pk, segInfos := range results {
		fmt.Printf("PK %d found in %d segments:\n", pk, len(segInfos))
		for _, info := range segInfos {
			fmt.Printf("  Segment %s - TS: %v\n", info.SegmentID, info.TSList)
			parentDir := filepath.Dir(info.SegmentID)
			ret, err := loadDeltaFromFileDir(chunkManager, parentDir)
			if err != nil {
				panic(err)
			}
			//fmt.Println("len of deltas", len(ret))
			tss, ok := ret[pk]
			if ok {
				fmt.Println("delete %d in %d", pk, tss)
			}
		}
	}
}
func queryDeltas(chunkManager storage.ChunkManager, pks []int64, fileDirs []string) {
	for _, fileDir := range fileDirs {
		ret, err := loadDeltaFromFileDir(chunkManager, fileDir)
		if err != nil {
			panic(err)
		}
		//fmt.Println("len of deltas", len(ret))
		for _, pk := range pks {
			tss, ok := ret[pk]
			if ok {
				fmt.Println("delete %d in %d at %d", pk, fileDir, tss)
			}
		}
	}
}

func countPKs(chunkManager storage.ChunkManager, fileDirs []string) {
	pksMap := make(map[int64]int64)
	multiCnt := 0
	for _, fileDir := range fileDirs {
		pkFile, err := extractPKFileFromDir(fileDir)
		if err != nil {
			panic(err)
		}
		if len(pkFile) == 0 {
			fmt.Println("empty PK file")
			continue
		}
		segment, err := LoadSegment(pkFile[0])
		if err != nil {
			panic(err)
		}
		for _, pair := range segment.Pairs {
			cnt, exist := pksMap[pair.PK]
			if exist {
				pksMap[pair.PK] = cnt + 1
			} else {
				pksMap[pair.PK] = 1
			}
		}
		for _, cnt := range pksMap {
			if cnt > 1 {
				multiCnt += 1
			}
		}
		fmt.Println("multiCnt:", multiCnt)
	}
	fmt.Println("final multiCnt:", multiCnt)
}
