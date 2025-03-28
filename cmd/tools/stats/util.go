package main

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/internal/storage"
	"os"
	"path/filepath"
	"strings"
)

func listFilesV1(fileDir string) []string {
	entries, err := os.ReadDir(fileDir)
	if err != nil {
		fmt.Println("failed to read dir:", err)
		return nil
	}
	fileNames := make([]string, 0, len(entries))

	for _, entry := range entries {
		info, _ := entry.Info() // 可选的详细信息
		fileNames = append(fileNames, fmt.Sprintf("%s/%s", fileDir, info.Name()))
	}
	return fileNames
}

func extractPKFileFromDir(fileDir string) ([]string, error) {
	entries, err := os.ReadDir(fileDir)
	if err != nil {
		fmt.Println("failed to read dir:", err)
		return nil, err
	}

	ret := make([]string, 0, len(entries))

	//statsType := storage.DefaultStatsType
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".pk" {
			ret = append(ret, fmt.Sprintf("%s/%s", fileDir, entry.Name()))
		}
	}
	return ret, nil
}

func extractStatsFileFromDir(fileDir string) ([]string, error) {
	entries, err := os.ReadDir(fileDir)
	if err != nil {
		fmt.Println("failed to read dir:", err)
		return nil, err
	}

	ret := make([]string, 0, len(entries))

	//statsType := storage.DefaultStatsType
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".bf_1" {
			ret = append(ret, fmt.Sprintf("%s/%s", fileDir, entry.Name()))
		} else if filepath.Ext(entry.Name()) == ".bf_0" {
			ret = append(ret, fmt.Sprintf("%s/%s", fileDir, entry.Name()))
		}
	}
	return ret, nil
}

func LoadStatsFromDir(chunkManager storage.ChunkManager, fileDir string) ([]*storage.PkStatistics, error) {
	bfFiles, err := extractStatsFileFromDir(fileDir)
	if err != nil {
		return nil, err
	}
	if len(bfFiles) == 0 {
		return nil, fmt.Errorf("no files to parse")
	}

	statsType := storage.DefaultStatsType
	for _, f := range bfFiles {
		if strings.HasSuffix(f, ".bf_0") {
			statsType = storage.DefaultStatsType
		} else if strings.HasSuffix(f, ".bf_1") {
			statsType = storage.CompoundStatsType
		}
	}

	// read historical PK filter
	values, err := chunkManager.MultiRead(context.Background(), bfFiles)
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

	//var size uint
	//var count int
	result := make([]*storage.PkStatistics, 0, len(stats))
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		//count += 1
		//size += stat.BF.Cap()
		result = append(result, pkStat)
	}
	//fmt.Println("Here, size:", size, " count:", count)
	return result, nil
}
