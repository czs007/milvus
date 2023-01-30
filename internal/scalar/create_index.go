package scalar

import (
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type IndexTask struct {
	//cm             storage.ChunkManager
	index          indexcgowrapper.CodecIndex
	savePaths      []string
	req            *indexpb.CreateJobRequest
	collectionID   UniqueID
	partitionID    UniqueID
	segmentID      UniqueID
	fieldID        UniqueID
	fieldData      storage.FieldData
	indexBlobs     []*storage.Blob
	newTypeParams  map[string]string
	newIndexParams map[string]string
	serializedSize uint64
	dataType       schemapb.DataType
	NumRows int64
	StrLen int
}

func newIndexJobRequest(numRows int64, rootPath, bucket string) *indexpb.CreateJobRequest {
	config := &indexpb.StorageConfig{
		BucketName: bucket,
		RootPath:   rootPath,
	}
	ret := &indexpb.CreateJobRequest{
		ClusterID:       "1",
		IndexFilePrefix: "indexes",
		IndexVersion:    1,
		IndexID:         1,
		IndexName:       "",
		StorageConfig:   config,
		NumRows:         numRows,
	}
	return ret
}

func NewIndexTask(segmentID int64, dataType schemapb.DataType, numRows int64, strLen int, rootPath, bucket string) *IndexTask {
	jobReq := newIndexJobRequest(int64(numRows), rootPath, bucket)
	//cm := storage.NewLocalChunkManager(storage.RootPath(rootPath))
	ret := &IndexTask{
		//cm:             cm,
		req:            jobReq,
		collectionID:   segmentID,
		partitionID:    segmentID,
		segmentID:      segmentID,
		fieldID:        101,
		//newTypeParams:  typeParams,
		newIndexParams: make(map[string]string),
		dataType: dataType,
		NumRows: numRows,
		StrLen: strLen,
	}
	return ret
}

func (it *IndexTask) LoadData(data storage.FieldData) error {
	it.fieldData = genFieldData(it.dataType, int(it.NumRows), it.StrLen)
	return nil
}

func (it *IndexTask) BuildIndex() error {
	dataset := indexcgowrapper.GenDataset(it.fieldData)
	dType := dataset.DType
	var err error
	if dType != schemapb.DataType_None {
		it.index, err = indexcgowrapper.NewCgoIndex(dType, it.newTypeParams, it.newIndexParams, it.req.GetStorageConfig())
		if err == nil {
			err = it.index.Build(dataset)
		}

		if err != nil {
			return err
		}
	}

	indexBlobs, err := it.index.Serialize()
	if err != nil {
		return err
	}

	// use serialized size before encoding
	it.serializedSize = 0
	for _, blob := range indexBlobs {
		it.serializedSize += uint64(len(blob.Value))
	}

	// early release index for gc, and we can ensure that Delete is idempotent.
	if err := it.index.Delete(); err != nil {
		return err
	}
	//
	//var serializedIndexBlobs []*storage.Blob
	//codec := storage.NewIndexFileBinlogCodec()
	//serializedIndexBlobs, err = codec.Serialize(
	//	it.req.BuildID,
	//	it.req.IndexVersion,
	//	it.collectionID,
	//	it.partitionID,
	//	it.segmentID,
	//	it.fieldID,
	//	it.newIndexParams,
	//	it.req.IndexName,
	//	it.req.IndexID,
	//	indexBlobs,
	//)
	//if err != nil {
	//	return err
	//}
	//it.indexBlobs = serializedIndexBlobs
	it.indexBlobs = indexBlobs
	return nil
}

//func (it *IndexTask) SaveIndexFiles(ctx context.Context) error {
//	blobCnt := len(it.indexBlobs)
//	savePaths := make([]string, blobCnt)
//	saveFileKeys := make([]string, blobCnt)
//
//	saveIndexFile := func(idx int) error {
//		blob := it.indexBlobs[idx]
//		savePath := metautil.BuildSegmentIndexFilePath(it.cm.RootPath(), it.req.BuildID,
//			it.req.IndexVersion, it.partitionID, it.segmentID, blob.Key)
//		saveFn := func() error {
//			return it.cm.Write(ctx, savePath, blob.Value)
//		}
//		if err := retry.Do(ctx, saveFn, retry.Attempts(5)); err != nil {
//			return err
//		}
//		savePaths[idx] = savePath
//		saveFileKeys[idx] = blob.Key
//		return nil
//	}
//
//	// If an error occurs, return the error that the task state will be set to retry.
//	if err := funcutil.ProcessFuncParallel(blobCnt, runtime.NumCPU(), saveIndexFile, "saveIndexFile"); err != nil {
//		log.Ctx(ctx).Error("saveIndexFile fail")
//		return err
//	}
//	it.savePaths = savePaths
//	return nil
//}
