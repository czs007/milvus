package main

import (
	"fmt"
	"github.com/milvus-io/milvus/internal/storage"
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
	found_exception := false
	for _, pair := range segment.Pairs {
		for _, bf := range bfs {
			if !bf.PkExist(storage.NewInt64PrimaryKey(pair.PK)) {
				fmt.Println(fmt.Sprintf("%s find %d not in bf", fileDir, pair.PK))
				found_exception = true
				break
			}
		}
		if found_exception {
			break
		}
	}
}

// 3499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf

func queryPKs(pks []int64, fileDirs []string) {
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
		}
	}
}
func queryDeltas(pks []int64, fileDirs []string) {

}
