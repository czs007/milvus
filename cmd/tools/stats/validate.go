package main

import (
	"fmt"
	"github.com/milvus-io/milvus/internal/storage"
)

func validatePKAndBF(chunkManager storage.ChunkManager, fileDir string) {
	pkFile, err := extractPKFileFromDir(fileDir)
	if err != nil {
		panic(err)
	}

	segment, err := LoadSegment(pkFile[0])
	if err != nil {
		panic(err)
	}

	bfs, err := LoadStatsFromDir(chunkManager, fileDir)
	if err != nil {
		panic(err)
	}
	for _, pair := range segment.Pairs {
		for _, bf := range bfs {
			if !bf.PkExist(storage.NewInt64PrimaryKey(pair.PK)) {
				fmt.Println(fmt.Sprintf("%s find %d not in bf", fileDir, pair.PK))
			}
		}
	}
}
