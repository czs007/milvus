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

package proxy

import (
	"encoding/binary"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	float16MinPositive float32 = 6.103515625e-05
)

func float32ToFloat16(data []float32) ([]byte, error) {
	ret := make([]byte, len(data)*2)
	for i, v := range data {
		if math.IsInf(float64(v), 0) || math.IsNaN(float64(v)) {
			// float16 supports Inf/NaN, but Milvus vector search doesn't
			// use the same error as Float16Vector validation
		}
		if v > 65504 || v < -65504 {
			return nil, merr.WrapErrParameterInvalidMsg("value at dimension %d (%v) exceeds float16 range [-65504, 65504]", i, v)
		}
		if v != 0 && (v < float16MinPositive && v > -float16MinPositive) {
			return nil, merr.WrapErrParameterInvalidMsg("value at dimension %d (%v) underflows float16 precision (min abs value: %v)", i, v, float16MinPositive)
		}
		u16 := typeutil.Float32ToFloat16(v)
		binary.LittleEndian.PutUint16(ret[i*2:], u16)
	}
	return ret, nil
}

func float32ToBFloat16(data []float32) ([]byte, error) {
	ret := make([]byte, len(data)*2)
	for i, v := range data {
		if math.IsInf(float64(v), 0) {
			return nil, merr.WrapErrParameterInvalidMsg("value at dimension %d is infinity, cannot convert to bfloat16", i)
		}
		if math.IsNaN(float64(v)) {
			return nil, merr.WrapErrParameterInvalidMsg("value at dimension %d is NaN, cannot convert to bfloat16", i)
		}
		u16 := typeutil.Float32ToBFloat16(v)
		binary.LittleEndian.PutUint16(ret[i*2:], u16)
	}
	return ret, nil
}

func convertFloat32ToFieldType(placeholder *milvuspb.PlaceholderValue, fieldType schemapb.DataType) error {
	if placeholder.GetType() != milvuspb.PlaceholderType_FloatVector {
		return nil
	}

	group := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(placeholder.GetValue(), group); err != nil {
		return merr.WrapErrParameterInvalidErr(err, "failed to unmarshal placeholder group")
	}

	for _, value := range group.GetPlaceholders() {
		if value.GetType() != milvuspb.PlaceholderType_FloatVector {
			continue
		}

		newValues := make([][]byte, 0, len(value.GetValues()))
		for _, v := range value.GetValues() {
			f32v := typeutil.BytesToFloat32Vector(v)
			var bytes []byte
			var err error
			switch fieldType {
			case schemapb.DataType_Float16Vector:
				bytes, err = float32ToFloat16(f32v)
			case schemapb.DataType_BFloat16Vector:
				bytes, err = float32ToBFloat16(f32v)
			default:
				return merr.WrapErrParameterInvalidMsg("vector type must be the same: field type %s, search type %s",
					fieldType.String(), placeholder.Type.String())
			}
			if err != nil {
				return err
			}
			newValues = append(newValues, bytes)
		}
		value.Values = newValues
	}

	var err error
	placeholder.Value, err = proto.Marshal(group)
	return err
}

func parseBinaryVector(placeholder *milvuspb.PlaceholderValue) ([][]byte, error) {
	group := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(placeholder.GetValue(), group); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "failed to unmarshal placeholder group")
	}
	if len(group.GetPlaceholders()) == 0 {
		return nil, nil
	}
	return group.GetPlaceholders()[0].GetValues(), nil
}

func parseFloat32Vector(placeholder *milvuspb.PlaceholderValue) ([][]float32, error) {
	group := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(placeholder.GetValue(), group); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "failed to unmarshal placeholder group")
	}
	if len(group.GetPlaceholders()) == 0 {
		return nil, nil
	}
	values := group.GetPlaceholders()[0].GetValues()
	ret := make([][]float32, 0, len(values))
	for i, v := range values {
		f32v, err := typeutil.VerifyFloat32Vector(v)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "failed to parse float32 vector at index %d", i)
		}
		ret = append(ret, f32v)
	}
	return ret, nil
}

func parseFloat32VectorToBytes(data []float32) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) > math.MaxInt32/4 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid float32 vector data length: %d", len(data))
	}
	return typeutil.Float32ArrayToBytes(data), nil
}
