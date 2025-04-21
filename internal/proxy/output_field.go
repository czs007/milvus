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
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"strings"
)

type outputFieldInfo struct {
	resultFields      []string
	userOutputFields  []string
	userDynamicFields []string
	pkFieldExplicit   bool
	outputFieldIDs    []UniqueID
	planNodes         map[string]*planpb.OutputFieldNode
}

// Support wildcard in output fields:
//
//	"*" - all fields
//
// For example, A and B are scalar fields, C and D are vector fields, duplicated fields will automatically be removed.
//
//	output_fields=["*"] 	 ==> [A,B,C,D]
//	output_fields=["*",A] 	 ==> [A,B,C,D]
//	output_fields=["*",C]    ==> [A,B,C,D]
//
// 4th return value is true if user requested pk field explicitly or using wildcard.
// if removePkField is true, pk field will not be include in the first(resultFieldNames)/second(userOutputFields)
// return value.
func translateOutputFields(outputFields []string, schema *schemaInfo, removePkField bool) (*outputFieldInfo, error) {
	var primaryFieldName string
	var dynamicField *schemapb.FieldSchema
	allFieldNameMap := make(map[string]*schemapb.FieldSchema)
	resultFieldNameMap := make(map[string]bool)
	resultFieldNames := make([]string, 0)
	userOutputFieldsMap := make(map[string]bool)
	userOutputFields := make([]string, 0)
	userDynamicFieldsMap := make(map[string]bool)
	userDynamicFields := make([]string, 0)
	useAllDynamicFields := false
	ret := &outputFieldInfo{
		planNodes: make(map[string]*planpb.OutputFieldNode),
	}
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			primaryFieldName = field.Name
		}
		if field.IsDynamic {
			dynamicField = field
		}
		allFieldNameMap[field.Name] = field
	}

	userRequestedPkFieldExplicitly := false

	for _, outputFieldName := range outputFields {
		outputFieldName = strings.TrimSpace(outputFieldName)
		if outputFieldName == primaryFieldName {
			userRequestedPkFieldExplicitly = true
		}
		schemaH, err := typeutil.CreateSchemaHelper(schema.CollectionSchema)
		if err != nil {
			return ret, err
		}

		outputFieldNode, err := planparserv2.ParseOutputField(schemaH, outputFieldName)
		if err != nil {
			return ret, fmt.Errorf("parse output field name failed: %s", outputFieldName)
		}
		curName := outputFieldNode.GetAlias()

		if outputFieldNode.GetSelectAll() {
			if _, exist := ret.planNodes[curName]; exist {
				continue
			}
			ret.planNodes[curName] = outputFieldNode
			userRequestedPkFieldExplicitly = true
			for fieldName, field := range allFieldNameMap {
				// skip Cold field and fields that can't be output
				if schema.IsFieldLoaded(field.GetFieldID()) && schema.CanRetrieveRawFieldData(field) {
					resultFieldNameMap[fieldName] = true
					userOutputFieldsMap[fieldName] = true
				}
			}
			useAllDynamicFields = true
			continue
		}
		if _, ok := ret.planNodes[curName]; ok {
			return ret, fmt.Errorf("duplicated field name: %s", curName)
		}
		ret.planNodes[curName] = outputFieldNode
		if field, ok := allFieldNameMap[outputFieldName]; ok {
			if !schema.CanRetrieveRawFieldData(field) {
				return ret, fmt.Errorf("not allowed to retrieve raw data of field %s", outputFieldName)
			}
			if schema.IsFieldLoaded(field.GetFieldID()) {
				resultFieldNameMap[outputFieldName] = true
				userOutputFieldsMap[outputFieldName] = true
			} else {
				return ret, fmt.Errorf("field %s is not loaded", outputFieldName)
			}
		} else {
			if schema.EnableDynamicField {
				if schema.IsFieldLoaded(dynamicField.GetFieldID()) {
					if !(len(outputFieldNode.GetExpr().GetColumnExpr().GetInfo().GetNestedPath()) == 1 &&
						outputFieldNode.GetExpr().GetColumnExpr().GetInfo().GetNestedPath()[0] == outputFieldName) {
						return ret, fmt.Errorf("parse output field name failed: %s", outputFieldName)
					}
					resultFieldNameMap[common.MetaFieldName] = true
					userOutputFieldsMap[outputFieldName] = true
					userDynamicFieldsMap[outputFieldName] = true
				} else {
					// TODO after cold field be able to fetched with chunk cache, this check shall be removed
					return ret, fmt.Errorf("field %s cannot be returned since dynamic field not loaded", outputFieldName)
				}
			} else {
				return ret, fmt.Errorf("field %s not exist", outputFieldName)
			}
		}

	}

	if removePkField {
		delete(resultFieldNameMap, primaryFieldName)
		delete(userOutputFieldsMap, primaryFieldName)
	}

	for fieldName := range resultFieldNameMap {
		resultFieldNames = append(resultFieldNames, fieldName)
	}
	for fieldName := range userOutputFieldsMap {
		userOutputFields = append(userOutputFields, fieldName)
	}
	if !useAllDynamicFields {
		for fieldName := range userDynamicFieldsMap {
			userDynamicFields = append(userDynamicFields, fieldName)
		}
	}
	ret.pkFieldExplicit = userRequestedPkFieldExplicitly
	ret.resultFields = resultFieldNames
	ret.userOutputFields = userOutputFields
	ret.userDynamicFields = userDynamicFields
	return ret, nil
}

// translateToOutputFieldIDs translates output fields name to output fields id.
// If no output fields specified, return only pk field
func translateToOutputFieldIDs(outputFields []string, schema *schemapb.CollectionSchema) ([]UniqueID, error) {
	outputFieldIDs := make([]UniqueID, 0, len(outputFields)+1)
	if len(outputFields) == 0 {
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				outputFieldIDs = append(outputFieldIDs, field.FieldID)
			}
		}
	} else {
		var pkFieldID UniqueID
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkFieldID = field.FieldID
			}
		}
		for _, reqField := range outputFields {
			var fieldFound bool
			for _, field := range schema.Fields {
				if reqField == field.Name {
					outputFieldIDs = append(outputFieldIDs, field.FieldID)
					fieldFound = true
					break
				}
			}
			if !fieldFound {
				return nil, fmt.Errorf("field %s not exist", reqField)
			}
		}

		// pk field needs to be in output field list
		var pkFound bool
		for _, outputField := range outputFieldIDs {
			if outputField == pkFieldID {
				pkFound = true
				break
			}
		}

		if !pkFound {
			outputFieldIDs = append(outputFieldIDs, pkFieldID)
		}
	}
	return outputFieldIDs, nil
}

func filterSystemFields(outputFieldIDs []UniqueID) []UniqueID {
	filtered := make([]UniqueID, 0, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		if !common.IsSystemField(outputFieldID) {
			filtered = append(filtered, outputFieldID)
		}
	}
	return filtered
}

func getOutputFieldIDs(schema *schemaInfo, outputFields []string) (outputFieldIDs []UniqueID, err error) {
	outputFieldIDs = make([]UniqueID, 0, len(outputFields))
	for _, name := range outputFields {
		id, ok := schema.MapFieldID(name)
		if !ok {
			return nil, fmt.Errorf("Field %s not exist", name)
		}
		outputFieldIDs = append(outputFieldIDs, id)
	}
	return outputFieldIDs, nil
}
