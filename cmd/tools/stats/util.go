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

func extractDeltaFileFromDir(fileDir string) ([]string, error) {
	entries, err := os.ReadDir(fileDir)
	if err != nil {
		fmt.Println("failed to read dir:", err)
		return nil, err
	}

	ret := make([]string, 0, len(entries))

	//statsType := storage.DefaultStatsType
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".delta" {
			ret = append(ret, fmt.Sprintf("%s/%s", fileDir, entry.Name()))
		}
	}
	return ret, nil
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

func loadDeltaFromFileDir(chunkManager storage.ChunkManager, fileDir string) (map[int64][]int64, error) {
	fileNames, err := extractDeltaFileFromDir(fileDir)
	if err != nil {
		return nil, err
	}
	if len(fileNames) == 0 {
		return nil, nil
	}
	var blobs []*storage.Blob
	dCodec := storage.DeleteCodec{}
	for _, fileName := range fileNames {
		value, err := chunkManager.Read(context.Background(), fileName)
		if err != nil {
			panic(err)
		}
		blob := &storage.Blob{
			Key:   fileName,
			Value: value,
		}
		blobs = append(blobs, blob)

		if len(blobs) == 0 {
			return nil, nil

		}
	}
	//fmt.Println("len of blobs:", len(blobs))
	_, _, deltaData, err := dCodec.Deserialize(blobs)
	if err != nil {
		return nil, err
	}
	l0DeleteRecords := make(map[int64][]int64) // pk => ts
	for i, pk := range deltaData.Pks {
		tss, ok := l0DeleteRecords[pk.GetValue().(int64)]
		if !ok {
			l0DeleteRecords[pk.GetValue().(int64)] = make([]int64, 0)
		}
		tss = append(tss, int64(deltaData.Tss[i]))
		l0DeleteRecords[pk.GetValue().(int64)] = tss
	}
	return l0DeleteRecords, nil
}
