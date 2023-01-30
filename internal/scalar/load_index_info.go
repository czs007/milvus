package scalar

/*
#cgo pkg-config: milvus_indexbuilder milvus_common milvus_segcore

#include <stdlib.h>	// free
#include "indexbuilder/index_c.h"

#include "segcore/load_index_c.h"
#include "common/binary_set_c.h"
*/
import "C"

import (
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"path/filepath"
	"unsafe"
)

// LoadIndexInfo is a wrapper of the underlying C-structure C.CLoadIndexInfo
type LoadIndexInfo struct {
	cLoadIndexInfo C.CLoadIndexInfo
}

func (li *LoadIndexInfo) appendIndexFile(filePath string) error {
	cIndexFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cIndexFilePath))

	status := C.AppendIndexFilePath(li.cLoadIndexInfo, cIndexFilePath)
	return HandleCStatus(&status, "AppendIndexIFile failed")
}

// appendIndexData appends binarySet index to cLoadIndexInfo
func (li *LoadIndexInfo) appendIndexData(bytesIndex [][]byte, sliceKeys []string) error {
	for _, indexPath := range sliceKeys {
		err := li.appendIndexFile(indexPath)
		if err != nil {
			return err
		}
	}

	var cBinarySet C.CBinarySet
	status := C.NewBinarySet(&cBinarySet)
	defer C.DeleteBinarySet(cBinarySet)

	if err := HandleCStatus(&status, "NewBinarySet failed"); err != nil {
		return err
	}

	for i, byteIndex := range bytesIndex {
		indexPtr := unsafe.Pointer(&byteIndex[0])
		indexLen := C.int64_t(len(byteIndex))
		binarySetKey := filepath.Base(sliceKeys[i])
		indexKey := C.CString(binarySetKey)
		status = C.AppendIndexBinary(cBinarySet, indexPtr, indexLen, indexKey)
		C.free(unsafe.Pointer(indexKey))
		if err := HandleCStatus(&status, "LoadIndexInfo AppendIndexBinary failed"); err != nil {
			return err
		}
	}

	status = C.AppendIndex(li.cLoadIndexInfo, cBinarySet)
	return HandleCStatus(&status, "AppendIndex failed")
}

// appendFieldInfo appends fieldID & fieldType to index
func (li *LoadIndexInfo) appendFieldInfo(collectionID int64, partitionID int64, segmentID int64, fieldID UniqueID, fieldType schemapb.DataType) error {
	cColID := C.int64_t(collectionID)
	cParID := C.int64_t(partitionID)
	cSegID := C.int64_t(segmentID)
	cFieldID := C.int64_t(fieldID)
	cintDType := uint32(fieldType)
	status := C.AppendFieldInfo(li.cLoadIndexInfo, cColID, cParID, cSegID, cFieldID, cintDType)
	return HandleCStatus(&status, "AppendFieldInfo failed")
}

// appendIndexParam append indexParam to index
func (li *LoadIndexInfo) appendIndexParam(indexKey string, indexValue string) error {
	cIndexKey := C.CString(indexKey)
	defer C.free(unsafe.Pointer(cIndexKey))
	cIndexValue := C.CString(indexValue)
	defer C.free(unsafe.Pointer(cIndexValue))
	status := C.AppendIndexParam(li.cLoadIndexInfo, cIndexKey, cIndexValue)
	return HandleCStatus(&status, "AppendIndexParam failed")
}

func (li *LoadIndexInfo) appendIndexInfo(indexID int64, buildID int64, indexVersion int64) error {
	cIndexID := C.int64_t(indexID)
	cBuildID := C.int64_t(buildID)
	cIndexVersion := C.int64_t(indexVersion)

	status := C.AppendIndexInfo(li.cLoadIndexInfo, cIndexID, cBuildID, cIndexVersion)
	return HandleCStatus(&status, "AppendIndexInfo failed")
}

func (li *LoadIndexInfo) appendLoadIndexInfo(bytesIndex [][]byte, indexInfo *querypb.FieldIndexInfo, collectionID int64, partitionID int64, segmentID int64, fieldType schemapb.DataType) error {
	fieldID := indexInfo.FieldID
	indexPaths := indexInfo.IndexFilePaths

	err := li.appendFieldInfo(collectionID, partitionID, segmentID, fieldID, fieldType)
	if err != nil {
		return err
	}

	err = li.appendIndexInfo(indexInfo.IndexID, indexInfo.BuildID, indexInfo.IndexVersion)
	if err != nil {
		return err
	}

	// some build params also exist in indexParams, which are useless during loading process
	indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)

	for key, value := range indexParams {
		err = li.appendIndexParam(key, value)
		if err != nil {
			return err
		}
	}

	err = li.appendIndexData(bytesIndex, indexPaths)
	return err
}

// deleteLoadIndexInfo would delete C.CLoadIndexInfo
func deleteLoadIndexInfo(info *LoadIndexInfo) {
	C.DeleteLoadIndexInfo(info.cLoadIndexInfo)
}

// newLoadIndexInfo returns a new LoadIndexInfo and error
func newLoadIndexInfo() (*LoadIndexInfo, error) {
	var cLoadIndexInfo C.CLoadIndexInfo

	cAddress := C.CString("a")
	cBucketName := C.CString("a")
	cAccessKey := C.CString("a")
	cAccessValue := C.CString("a")
	cRootPath := C.CString("a")
	cStorageType := C.CString("minio")
	cIamEndPoint := C.CString("a")
	defer C.free(unsafe.Pointer(cAddress))
	defer C.free(unsafe.Pointer(cBucketName))
	defer C.free(unsafe.Pointer(cAccessKey))
	defer C.free(unsafe.Pointer(cAccessValue))
	defer C.free(unsafe.Pointer(cRootPath))
	defer C.free(unsafe.Pointer(cStorageType))
	defer C.free(unsafe.Pointer(cIamEndPoint))
	storageConfig := C.CStorageConfig{
		address:          cAddress,
		bucket_name:      cBucketName,
		access_key_id:    cAccessKey,
		access_key_value: cAccessValue,
		remote_root_path: cRootPath,
		storage_type:     cStorageType,
		iam_endpoint:     cIamEndPoint,
		useSSL:           C.bool(false),
		useIAM:           C.bool(false),
	}

	status := C.NewLoadIndexInfo(&cLoadIndexInfo, storageConfig)
	if err := HandleCStatus(&status, "NewLoadIndexInfo failed"); err != nil {
		return nil, err
	}
	return &LoadIndexInfo{cLoadIndexInfo: cLoadIndexInfo}, nil
}
