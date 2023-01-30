package scalar

/*
#cgo pkg-config: milvus_common milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"

)

// Collection is a wrapper of the underlying C-structure C.CCollection
type Collection struct {
	collectionPtr C.CCollection
	id            UniqueID
	schema        *schemapb.CollectionSchema
}

type Segment struct {
	segmentPtr  C.CSegmentInterface
	collection  *Collection
	segmentID   UniqueID
	dataType    schemapb.DataType
	RowCount    int64
	indexParams []*commonpb.KeyValuePair
	typeParams  map[string]string
	//indexBlobs  storage.BlobList
	indexBytes    [][]byte
	indexKeys     []string
	VMRSSMemSizes []int64
	MemStats      []*runtime.MemStats
	NumCopy       int
	StrLen        int
	fieldData      storage.FieldData
	schemaFieldData *schemapb.FieldData
	fieldDataBlob []byte
}

func NewSegment(segmentID UniqueID, dataType schemapb.DataType, numCopy int, numRow int, strLen int) (*Segment, error) {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/

	schema := &schemapb.CollectionSchema{
		Name:        fmt.Sprintf("segment_%d", segmentID),
		Description: "",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	for i := 1; i <= numCopy; i++ {
		fieldSchema := &schemapb.FieldSchema{
			FieldID:  common.StartOfUserFieldID + int64(i),
			Name:     fmt.Sprintf("field_%d", i),
			DataType: dataType,
		}

		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = append(fieldSchema.TypeParams, &commonpb.KeyValuePair{
				Key:   "max_length",
				Value: "65536",
			})
		}
		schema.Fields = append(schema.Fields, fieldSchema)
	}

	schemaBlob := proto.MarshalTextString(schema)

	cSchemaBlob := C.CString(schemaBlob)
	cCollection := C.NewCollection(cSchemaBlob)

	collection := &Collection{
		collectionPtr: cCollection,
		id:            segmentID,
		schema:        schema,
	}
	C.free(unsafe.Pointer(cSchemaBlob))

	var segmentPtr C.CSegmentInterface

	segmentPtr = C.NewSegment(collection.collectionPtr, C.Sealed, C.int64_t(segmentID))

	var indexParams []*commonpb.KeyValuePair

	if dataType == schemapb.DataType_VarChar || dataType == schemapb.DataType_String {
		indexParams = append(indexParams, &commonpb.KeyValuePair{
			Key:   "index_type",
			Value: "Trie",
		})
	} else {
		indexParams = append(indexParams, &commonpb.KeyValuePair{
			Key:   "index_type",
			Value: "STL_SORT",
		})
	}
	var segment = &Segment{
		segmentPtr:  segmentPtr,
		segmentID:   segmentID,
		collection:  collection,
		NumCopy:     numCopy,
		RowCount:    int64(numRow),
		dataType:    dataType,
		StrLen:      strLen,
		indexParams: indexParams,
	}

	return segment, nil
}

func (s *Segment) Delete() {
	cPtrCollection := s.collection.collectionPtr
	C.DeleteCollection(cPtrCollection)
	s.collection.collectionPtr = nil
	s.collection = nil

	var cPtr C.CSegmentInterface
	// wait all read ops finished
	cPtr = s.segmentPtr
	s.segmentPtr = nil

	if cPtr == nil {
		return
	}
	C.DeleteSegment(cPtr)
}

func parseVMRSS(s string) int64 {
	retValues := strings.Split(s, ";")
	retValue, err := strconv.ParseInt(retValues[3], 10, 64)
	if err != nil {
		panic(err)
	}
	return retValue
}

func memToMB(s string) string {
	//ss << s.VmPeak << ";"
	//<< s.VmSize << ";"
	//<< s.VmHWM  << ";"
	//<< s.VmRSS  << ";"
	//<< s.VmData << ";"
	//<< s.VmPTE  << ";"
	titiles := []string{
		"VmPeak",
		"VmSize",
		"VmHWM",
		"VmRSS",
		"VmData",
		"VmPTE",
	}
	valueStr := ""

	retValues := strings.Split(s, ";")
	//var values []int64
	//values := []int64 {}
	for j, a := range retValues {
		//fmt.Println(retValues)
		if a == "" {
			continue
		}
		aMB, err := strconv.ParseInt(a, 10, 64)
		if err != nil {
			panic(err)
		}
		//aMB := aValue/1024/104
		valueStr += titiles[j]
		valueStr += ":"
		valueStr += fmt.Sprintf("%d", aMB)
		valueStr += " "
		//values = append(values, i)
	}
	//valueStr += "VmPeak:"
	//fmt.Println(s)
	//fmt.Println("HVMRSS:", parseVMRSS(s))
	return valueStr
}

func (s *Segment) snapMemStat() {
	runtime.GC()
	debug.FreeOSMemory()
	s.MemStats = append(s.MemStats, snapMemStats())
	//PrintMemUsage()
	ret := getMemStatStr()
	//fmt.Println(memToMB(ret))
	s.VMRSSMemSizes = append(s.VMRSSMemSizes, parseVMRSS(ret))
	//s.MemStats = append(s.MemStats, )
	//fmt.Println("haha:", ret)
}

func (s *Segment) LoadData() {
	s.fieldData = genFieldData(s.dataType, int(s.RowCount), s.StrLen)
	s.schemaFieldData = transferFieldDataToScheamFieldData(s.fieldData, s.dataType, int(s.RowCount))
	s.fieldData = nil

	//s.schemaFieldData.FieldId = int64(common.StartOfUserFieldID + index)
	var err error
	s.fieldDataBlob, err = proto.Marshal(s.schemaFieldData)
	if err != nil {
		panic("LoadData Failed")
	}
	s.schemaFieldData = nil
}

func (s *Segment) BuildIndex() {
	{
		task := NewIndexTask(s.segmentID, s.dataType, s.RowCount, s.StrLen, "", "")
		task.LoadData(s.fieldData)
		task.BuildIndex()
		for _, blob := range task.indexBlobs {
			s.indexBytes = append(s.indexBytes, blob.Value)
			s.indexKeys = append(s.indexKeys, blob.Key)
		}
	}
	s.snapMemStat()
}

func (s *Segment) LoadAllRawData() error {
	s.snapMemStat()
	for i := 1; i <= s.NumCopy; i++ {
		err := s.segmentLoadRawData(i)
		if err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	s.snapMemStat()
	return nil
}

func (s *Segment) segmentLoadRawData(index int) error {
	loadInfo := C.CLoadFieldDataInfo{
		field_id:  C.int64_t(int64(common.StartOfUserFieldID + index)),
		blob:      (*C.uint8_t)(unsafe.Pointer(&s.fieldDataBlob[0])),
		blob_size: C.uint64_t(len(s.fieldDataBlob)),
		row_count: C.int64_t(s.RowCount),
	}

	status := C.LoadFieldData(s.segmentPtr, loadInfo)

	if err := HandleCStatus(&status, "LoadFieldData failed"); err != nil {
		return err
	}

	return nil
}

func (s *Segment) LoadAllIndexData() error {
	for i := 1; i <= s.NumCopy; i++ {
		err := s.segmentLoadIndexData(i, s.indexBytes, s.indexKeys)
		if err != nil {
			return err
		}
		s.snapMemStat()
		time.Sleep(time.Second)
	}
	return nil
}

func (s *Segment) segmentLoadIndexData(index int, bytesIndex [][]byte, sliceKeys []string) error {
	loadIndexInfo, err := newLoadIndexInfo()
	defer func() {
		deleteLoadIndexInfo(loadIndexInfo)
	}()

	if err != nil {
		return err
	}
	indexInfo := &querypb.FieldIndexInfo{
		FieldID:        int64(common.StartOfUserFieldID + index),
		IndexName:      fmt.Sprintf("segment-%d", s.segmentID),
		IndexID:        s.segmentID,
		BuildID:        s.segmentID,
		IndexParams:    s.indexParams,
		IndexFilePaths: sliceKeys,
		IndexVersion:   1,
		NumRows:        s.RowCount,
	}
	//fmt.Println("IndexFilePaths:", sliceKeys)
	err = loadIndexInfo.appendLoadIndexInfo(bytesIndex, indexInfo, s.segmentID, s.segmentID, s.segmentID, s.dataType)
	if err != nil {
		return err
	}

	status := C.UpdateSealedSegmentIndex(s.segmentPtr, loadIndexInfo.cLoadIndexInfo)

	if err := HandleCStatus(&status, "UpdateSealedSegmentIndex failed"); err != nil {
		return err
	}
	return nil
}

func getMemStatStr() string {
	retChar := C.GetMemStats()
	ret := C.GoString(retChar)
	defer C.free(unsafe.Pointer(retChar))
	return ret
}
