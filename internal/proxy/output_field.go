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
	"google.golang.org/protobuf/proto"
	"strings"
)

type outputFieldInfo struct {
	resultFields        []string
	userOutputFields    []string
	userDynamicFields   []string
	pkFieldExplicit     bool
	outputFieldIDs      []UniqueID
	primaryFieldName    string
	useAllDynamicFields bool

	primaryFieldID UniqueID
	dynamicField   *schemapb.FieldSchema
	schemaH        *typeutil.SchemaHelper

	allFieldNameMap map[string]*schemapb.FieldSchema
	planNodes       map[string]*planpb.OutputFieldNode
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
	resultFieldNameMap := make(map[string]bool)
	resultFieldNames := make([]string, 0)
	userOutputFieldsMap := make(map[string]bool)
	userOutputFields := make([]string, 0)
	userDynamicFieldsMap := make(map[string]bool)
	userDynamicFields := make([]string, 0)
	useAllDynamicFields := false
	ret := &outputFieldInfo{
		planNodes:       make(map[string]*planpb.OutputFieldNode),
		allFieldNameMap: make(map[string]*schemapb.FieldSchema),
	}
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			ret.primaryFieldName = field.Name
			ret.primaryFieldID = field.GetFieldID()
		}
		if field.IsDynamic {
			ret.dynamicField = field
		}
		ret.allFieldNameMap[field.Name] = field
	}

	ret.pkFieldExplicit = false
	schemaH, err := typeutil.CreateSchemaHelper(schema.CollectionSchema)
	if err != nil {
		return nil, err
	}
	ret.schemaH = schemaH

	for _, outputFieldName := range outputFields {
		outputFieldName = strings.TrimSpace(outputFieldName)
		if outputFieldName == ret.primaryFieldName {
			ret.pkFieldExplicit = true
		}
		outputFieldNode, err := planparserv2.ParseOutputField(schemaH, outputFieldName)
		if err != nil {
			return nil, fmt.Errorf("parse output field name failed: %s", outputFieldName)
		}

		alias := outputFieldNode.GetAlias()
		if oldNode, ok := ret.planNodes[alias]; ok {
			if !proto.Equal(outputFieldNode, oldNode) {
				return nil, fmt.Errorf("conflict output field name: %s", alias)
			}
			// If the output field is identical, idempotent processing will be performed here.
			continue
		}

		switch {
		case outputFieldNode.GetSelectAll():
			err = processSelectAllOutputField(ret, outputFieldNode, schema)
			if err != nil {
				return nil, err
			}
		default:
			expr := outputFieldNode.GetExpr().GetExpr()
			switch expr.(type) {
			case *planpb.OutputFieldExpr_ColumnExpr:
				err = processColumnOutputField(ret, outputFieldNode, schema)
				if err != nil {
					return nil, err
				}
			case *planpb.OutputFieldExpr_CountExpr:
				err = processCountOutputField(ret, outputFieldNode)
				if err != nil {
					return nil, err
				}
			case *planpb.OutputFieldExpr_ScoreExpr:
				err = processScoreOutputField(ret, outputFieldNode)
				if err != nil {
					return nil, err
				}
			case *planpb.OutputFieldExpr_DistanceExpr:
				err = processDistanceOutputField(ret, outputFieldNode)
				if err != nil {
					return nil, err
				}
			default:
				// process unknown output field
				return nil, fmt.Errorf("unsupported output field: %s", alias)
			}
		}
	}

	if removePkField {
		delete(resultFieldNameMap, ret.primaryFieldName)
		delete(userOutputFieldsMap, ret.primaryFieldName)
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
			return nil, fmt.Errorf("field %s not exist", name)
		}
		outputFieldIDs = append(outputFieldIDs, id)
	}
	return outputFieldIDs, nil
}

func processSelectAllOutputField(ret *outputFieldInfo, node *planpb.OutputFieldNode, schema *schemaInfo) error {
	ret.pkFieldExplicit = true
	ret.useAllDynamicFields = true

	for fieldName, field := range ret.allFieldNameMap {
		// skip Cold field and fields that can't be output
		if schema.IsFieldLoaded(field.GetFieldID()) && schema.CanRetrieveRawFieldData(field) {
			tempOutputFieldNode, err := planparserv2.ParseOutputField(ret.schemaH, fieldName)
			if err != nil {
				return fmt.Errorf("parse output field name failed: %s", fieldName)
			}
			// for different output field, alias should not conflict
			if exitOutputNode, exist := ret.planNodes[fieldName]; exist {
				if !proto.Equal(tempOutputFieldNode, exitOutputNode) {
					return fmt.Errorf("duplicated output field name: %s", fieldName)
				}
				continue
			}
			ret.planNodes[fieldName] = tempOutputFieldNode
		}
	}
	ret.planNodes[node.GetAlias()] = node
	return nil
}

func processColumnOutputField(ret *outputFieldInfo, node *planpb.OutputFieldNode, schema *schemaInfo) error {
	outputFieldName := node.GetName()
	field, ok := ret.allFieldNameMap[outputFieldName]
	if !ok {
		if schema.EnableDynamicField {
			if schema.IsFieldLoaded(ret.dynamicField.GetFieldID()) {
				if !(len(node.GetExpr().GetColumnExpr().GetInfo().GetNestedPath()) == 1 &&
					node.GetExpr().GetColumnExpr().GetInfo().GetNestedPath()[0] == outputFieldName) {
					return fmt.Errorf("parse output field name failed: %s", outputFieldName)
				}
			} else {
				// TODO after cold field be able to fetched with chunk cache, this check shall be removed
				return fmt.Errorf("field %s cannot be returned since dynamic field not loaded", outputFieldName)
			}
		} else {
			return fmt.Errorf("field %s not exist", outputFieldName)
		}
	} else {
		if !schema.CanRetrieveRawFieldData(field) {
			return fmt.Errorf("not allowed to retrieve raw data of field %s", outputFieldName)
		}
		if !schema.IsFieldLoaded(field.GetFieldID()) {
			return fmt.Errorf("field %s is not loaded", outputFieldName)
		}
	}
	ret.planNodes[node.GetAlias()] = node
	return nil
}

func processCountOutputField(ret *outputFieldInfo, node *planpb.OutputFieldNode) error {
	if !node.GetExpr().GetCountExpr().GetAsterisk() {
		return fmt.Errorf("only count(*) is supported in the output fields")
	}
	// currently, we only support count(*)
	if len(ret.planNodes) > 0 {
		return fmt.Errorf("count must appear alone in the output fields")
	}
	ret.planNodes[node.GetAlias()] = node
	return nil
}

func processDistanceOutputField(ret *outputFieldInfo, node *planpb.OutputFieldNode) error {
	name := node.GetName()
	expr := node.GetExpr().GetDistanceExpr()
	fieldName := expr.GetName()
	field, ok := ret.allFieldNameMap[fieldName]
	if !ok {
		return fmt.Errorf("in the %s, field %s not exist", name, fieldName)
	}
	dataType := field.GetDataType()
	if !typeutil.IsVectorType(dataType) && !typeutil.IsStringType(dataType) {
		return fmt.Errorf("in the %s, can not call distance on field %s", name, fieldName)
	}
	ret.planNodes[node.GetAlias()] = node
	return nil
}

func processScoreOutputField(ret *outputFieldInfo, node *planpb.OutputFieldNode) error {
	name := node.GetName()
	expr := node.GetExpr().GetScoreExpr()
	fieldName := expr.GetName()
	field, ok := ret.allFieldNameMap[fieldName]
	if !ok {
		return fmt.Errorf("in the %s, field %s not exist", name, fieldName)
	}
	dataType := field.GetDataType()
	if !typeutil.IsVectorType(dataType) && !typeutil.IsStringType(dataType) {
		return fmt.Errorf("in the %s, can not call score on field %s", name, fieldName)
	}
	ret.planNodes[node.GetAlias()] = node
	return nil
}
