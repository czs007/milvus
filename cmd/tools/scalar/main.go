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

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	scalar "github.com/milvus-io/milvus/internal/scalar"
)

var (
	datatype  string // int8, int16, ..., int64, float32, string
	strLength int    // 1-655535
	num       int
	loadNum   int
)

func init() {
	flag.StringVar(&datatype, "type", "", "scala datatype, value can be [int8, ..., int64, float32, string]")
	flag.IntVar(&strLength, "str_len", 0, "len fo string, required if type is string")
	flag.IntVar(&num, "num", 0, "num of rows, required")
	flag.IntVar(&loadNum, "load_num", 0, "round of load, required")
}

func convertDataTypeSize(typeStr string, strLen int) (int, error) {
	validDataTypes := make(map[string]int)
	validDataTypes["int8"] = 1
	validDataTypes["int16"] = 2
	validDataTypes["int32"] =  4
	validDataTypes["int64"] = 8
	validDataTypes["float32"] = 4
	validDataTypes["float"] = 4
	validDataTypes["double"] = 8
	validDataTypes["string"] = strLen

	dSize, typeMatch := validDataTypes[typeStr]
	if !typeMatch {
		return 0, fmt.Errorf("wrong data type")
	}

	return dSize, nil
}

func convertDataType(typeStr string) (schemapb.DataType, error) {
	validDataTypes := make(map[string]schemapb.DataType)
	validDataTypes["int8"] = schemapb.DataType_Int8
	validDataTypes["int16"] = schemapb.DataType_Int16
	validDataTypes["int32"] = schemapb.DataType_Int32
	validDataTypes["int64"] = schemapb.DataType_Int64
	validDataTypes["float32"] = schemapb.DataType_Float
	validDataTypes["float"] = schemapb.DataType_Float
	validDataTypes["double"] = schemapb.DataType_Double
	validDataTypes["string"] = schemapb.DataType_VarChar

	dType, typeMatch := validDataTypes[typeStr]
	if !typeMatch {
		return schemapb.DataType_None, fmt.Errorf("wrong data type")
	}
	return dType, nil
}

func checkParams() error {
	if num < 1 {
		return fmt.Errorf("wrong num, should be big than 0")
	}
	if loadNum < 1 {
		return fmt.Errorf("wrong load_num, should be big than 0")
	}
	_, err := convertDataType(datatype)
	if err != nil {
		return fmt.Errorf("wrong data type")
	}
	if datatype == "string" {
		if strLength < 1 {
			return fmt.Errorf("wrong str_len should be big than 0")
		}
	}
	return nil
}

func main() {
	flag.Parse()

	var Usage = func() {
		flag.PrintDefaults()
	}
	err := checkParams()
	if err != nil {
		fmt.Println(err.Error())
		Usage()
		return
	}
	dSize, err:= convertDataTypeSize(datatype,strLength)
	if err != nil {
		panic(err)
	}
	dType, _ := convertDataType(datatype)
	if dType == schemapb.DataType_VarChar {
		typeParams := make(map[string]string)
		typeParams["max_length"] = "65536"
	}

	s, err := scalar.NewSegment(100, dType, loadNum, num, strLength)
	s.LoadData()
//	s.BuildIndex()
	//err = s.LoadAllIndexData()
	err = s.LoadAllRawData()
	if err != nil {
		panic(err)
	}
	fmt.Println(s.VMRSSMemSizes)
	s.Delete()
	time.Sleep(time.Second)
	realSize := s.VMRSSMemSizes[1] - s.VMRSSMemSizes[0]
	var ratioFlag bool
	oldDSize := dSize
	var shouldPrintStrLen bool
	if dType == schemapb.DataType_VarChar {
		shouldPrintStrLen = true
		if (dSize <= 15) {
			dSize = 15
			fmt.Println("align dSize to 15")
			ratioFlag = true
		}
	}
	rawSize := num * dSize
	ratio := float64(realSize) / float64(rawSize) / float64(loadNum)
	valueStr := fmt.Sprintf("type:%s;row:%d;size:%f;ratio:%v", datatype, num, float64(s.VMRSSMemSizes[1]-s.VMRSSMemSizes[0])/float64(loadNum), ratio)
	if shouldPrintStrLen {
		valueStr += fmt.Sprintf(":str_len:%d", strLength)
	}
	if ratioFlag {
		rawSize := num * oldDSize
		ratio2 := float64(realSize) / float64(rawSize) / float64(loadNum)
		valueStr += fmt.Sprintf(":ratio2:%v", ratio2)
	}
	fmt.Println(valueStr)
	//fmt.Println("Size:", s.VMRSSMemSizes[1] - s.VMRSSMemSizes[0])
	//if err := storage.PrintBinlogFiles(os.Args[1:]); err != nil {
	//	fmt.Printf("error: %s\n", err.Error())
	//} else {
	//	fmt.Printf("print binlog complete.\n")
	//}
}
