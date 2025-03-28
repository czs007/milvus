package main

import (
	"fmt"
	"github.com/milvus-io/milvus/internal/storage"
	"os"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("usage: binlog file1 file2 ...")
	}
	fileDir := os.Args[1]

	fileNames := listFilesV1(fileDir)
	//fmt.Println("fileNames:", fileNames)
	//files := []string

	if false {
		manager := storage.NewLocalChunkManager()
		for _, fileDir := range fileNames {
			validatePKAndBF(manager, fileDir)
		}
	}
	if false {
		manager := storage.NewLocalChunkManager()
		queryPKs(manager, []int64{408629564}, fileNames)
	}
	if true {
		manager := storage.NewLocalChunkManager()
		queryDeltas(manager, []int64{408629564}, fileNames)
	}
}
