// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scalar

/*
#cgo pkg-config: milvus_segcore milvus_storage milvus_common

#include "segcore/collection_c.h"
#include "common/type_c.h"
#include "segcore/segment_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"math"
	"math/rand"
	"runtime"
	"time"
	"unsafe"
)

var r *rand.Rand

func init() {
	//rand.Seed(time.Now().UnixNano())
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func bToKb(b uint64) uint64 {
	return b / 1024
}

// HandleCStatus deals with the error returned from CGO
func HandleCStatus(status *C.CStatus, extraInfo string) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorName, ok := commonpb.ErrorCode_name[int32(errorCode)]
	if !ok {
		errorName = "UnknownError"
	}
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	finalMsg := fmt.Sprintf("[%s] %s", errorName, errorMsg)
	return errors.New(finalMsg)
}

func snapMemStats() *runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return &m
}

func PrintMemUsage() {
	m := snapMemStats()
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB, %v KB", bToMb(m.Alloc), bToKb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB, %v KB", bToMb(m.TotalAlloc), bToKb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB, %v KB", bToMb(m.Sys), bToKb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func randIntRange(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func randInt8Range(min, max int8) int8 {
	return int8(randIntRange(int(min), int(max)))
}

func randInt16Range(min, max int16) int16 {
	return int16(randIntRange(int(min), int(max)))
}

func randInt32Range(min, max int32) int32 {
	return rand.Int31n(max-min+1) + min
}

func randInt64Range(min, max int64) int64 {
	return rand.Int63n(max-min+1) + min
}

func randFloat32Range(min, max float32) float32 {
	return min + rand.Float32()*(max-min)
}

func randFloat64Range(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

var letterRunes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandomBytes returns a batch of random string
func RandomBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return b
}

// RandomString returns a batch of random string
func RandomString(n int) string {
	return string(RandomBytes(n))
}


func transferFieldDataToScheamFieldData(rawData storage.FieldData, dataType schemapb.DataType, numRows int) *schemapb.FieldData {

	var fieldData *schemapb.FieldData

	switch rawData := rawData.(type) {
		case *storage.BoolFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Bool,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *storage.Int8FieldData:
			int32Data := make([]int32, len(rawData.Data))
			for index, v := range rawData.Data {
				int32Data[index] = int32(v)
			}
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int8,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: int32Data,
							},
						},
					},
				},
			}
		case *storage.Int16FieldData:
			int32Data := make([]int32, len(rawData.Data))
			for index, v := range rawData.Data {
				int32Data[index] = int32(v)
			}
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int16,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: int32Data,
							},
						},
					},
				},
			}
		case *storage.Int32FieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int32,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *storage.Int64FieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int64,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *storage.FloatFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Float,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *storage.DoubleFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Double,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *storage.StringFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_VarChar,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *storage.FloatVectorFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_FloatVector,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: rawData.Data,
							},
						},
						Dim: int64(rawData.Dim),
					},
				},
			}
		case *storage.BinaryVectorFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_BinaryVector,
//				FieldId: fieldID,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: rawData.Data,
						},
						Dim: int64(rawData.Dim),
					},
				},
			}
		default:
			return nil
		}
	return fieldData
}

func genFieldData(dataType schemapb.DataType, numRows int, strLen int) storage.FieldData {

	var ret storage.FieldData
	switch dataType {
	case schemapb.DataType_Bool:
		ret1 := &storage.BoolFieldData{
			Data: make([]bool, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = rand.Intn(2) == 1
		}
		ret = ret1
	case schemapb.DataType_Float:
		ret1 := &storage.FloatFieldData{
			Data: make([]float32, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = randFloat32Range(0, 1000.0)
		}
		ret = ret1
	case schemapb.DataType_Double:
		ret1 := &storage.DoubleFieldData{
			Data: make([]float64, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = randFloat64Range(0, 1000.0)
		}
		ret = ret1
	case schemapb.DataType_Int8:
		ret1 := &storage.Int8FieldData{
			Data: make([]int8, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = randInt8Range(0, math.MaxInt8-1)
		}
		ret = ret1
	case schemapb.DataType_Int16:
		ret1 := &storage.Int16FieldData{
			Data: make([]int16, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = randInt16Range(0, math.MaxInt16-1)
		}
		ret = ret1
	case schemapb.DataType_Int32:
		ret1 := &storage.Int32FieldData{
			Data: make([]int32, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = randInt32Range(0, math.MaxInt32-1)
		}
		ret = ret1
	case schemapb.DataType_Int64:
		ret1 := &storage.Int64FieldData{
			Data: make([]int64, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = randInt64Range(0, math.MaxInt64-1)
		}
		ret = ret1
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		ret1 := &storage.StringFieldData{
			Data: make([]string, numRows),
		}
		for i := 0; i < numRows; i++ {
			ret1.Data[i] = RandomString(strLen)
		}
		ret = ret1
	default:
		return nil
	}
	return ret
}
