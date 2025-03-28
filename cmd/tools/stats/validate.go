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
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893200 not in bf
//.//456889800282293499 find 434893201 not in bf
//.//456889800282293499 find 434893201 not in bf
//.//456889800282293499 find 434893201 not
