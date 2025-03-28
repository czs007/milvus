package main

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/internal/storage"
	"os"
	"strings"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("usage: binlog file1 file2 ...")
	}
	fileDir := os.Args[1]

	fileNames := listFilesV1(fileDir)
	fmt.Println("fileNames:", fileNames)

	if false {
		manager := storage.NewLocalChunkManager()

		if _, err := ParseStats(manager, fileNames); err != nil {
			fmt.Printf("error: %s\n", err.Error())
		} else {
			fmt.Printf("print binlog complete.\n")
		}
	}

}

func listFilesV1(fileDir string) []string {
	entries, err := os.ReadDir(fileDir)
	if err != nil {
		fmt.Println("failed to read dir:", err)
		return nil
	}
	fileNames := make([]string, 0, len(entries))

	for _, entry := range entries {
		info, _ := entry.Info() // 可选的详细信息
		fileNames = append(fileNames, info.Name())
		//fmt.Printf("- %-25s %8d bytes\n", entry.Name(), info.Size())
	}
	return fileNames
}

func ParseStats(chunkManager storage.ChunkManager, files []string) ([]*storage.PkStatistics, error) {

	if len(files) == 0 {
		return nil, fmt.Errorf("no files to parse")
	}
	statsType := storage.DefaultStatsType
	for _, f := range files {
		if strings.HasSuffix(f, ".bf_0") {
			statsType = storage.DefaultStatsType
		} else if strings.HasSuffix(f, ".bf_1") {
			statsType = storage.CompoundStatsType
		}
	}

	// read historical PK filter
	values, err := chunkManager.MultiRead(context.Background(), files)
	if err != nil {
		return nil, err
	}
	blobs := make([]*storage.Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &storage.Blob{Value: values[i]})
	}

	var stats []*storage.PrimaryKeyStats
	if statsType == storage.CompoundStatsType {
		stats, err = storage.DeserializeStatsList(blobs[0])
		if err != nil {
			return nil, err
		}
	} else {
		stats, err = storage.DeserializeStats(blobs)
		if err != nil {
			return nil, err
		}
	}

	var size uint
	result := make([]*storage.PkStatistics, 0, len(stats))
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		result = append(result, pkStat)
	}
	fmt.Println("Here,", size)
	return result, nil
}
