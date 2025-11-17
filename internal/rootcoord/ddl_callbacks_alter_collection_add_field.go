package rootcoord

import (
	"context"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/timestamptz"
	"go.uber.org/zap"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastAlterCollectionForAddField broadcasts the put collection message for add field.
func (c *Core) broadcastAlterCollectionForAddField(ctx context.Context, req *milvuspb.AddCollectionFieldRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// check if the collection is created.
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	// check if the field schema is illegal.
	fieldSchema := &schemapb.FieldSchema{}
	if err = proto.Unmarshal(req.Schema, fieldSchema); err != nil {
		return errors.Wrap(err, "failed to unmarshal field schema")
	}
	if err := checkFieldSchema([]*schemapb.FieldSchema{fieldSchema}); err != nil {
		return errors.Wrap(err, "failed to check field schema")
	}

	if fieldSchema.GetDataType() == schemapb.DataType_Timestamptz {
		timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, coll.Properties)
		if !exist {
			timezone = common.DefaultTimezone
		}
		timestamptz.CheckAndRewriteTimestampTzDefaultValueForFieldSchema(fieldSchema, timezone)
	}

	// check if the field already exists
	for _, field := range coll.Fields {
		if field.Name == fieldSchema.Name {
			// TODO: idempotency check here.
			return merr.WrapErrParameterInvalidMsg("field already exists, name: %s", fieldSchema.Name)
		}
	}

	// assign a new field id.
	fieldSchema.FieldID = nextFieldID(coll)
	// build new collection schema.
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: coll.EnableDynamicField,
		Properties:         coll.Properties,
		Version:            coll.SchemaVersion + 1,
	}
	schema.Fields = append(schema.Fields, fieldSchema)

	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	// broadcast the put collection v2 message.
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:         coll.DBID,
			CollectionId: coll.CollectionID,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema},
			},
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: schema,
			},
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// checkAndRewriteTimestampTzDefaultValueForFieldSchema processes a single FieldSchema
// to validate and rewrite the default value specifically for TIMESTAMPTZ fields.
//
// The function ensures the default value (initially a string) is correctly converted
// and stored internally as an absolute UTC microsecond (int64) value.
//
// Parameters:
//
//	fieldSchema: The specific FieldSchema object to be processed.
//	collectionTimezone: The collection-level default timezone string (e.g., "UTC", "Asia/Shanghai")
//	                    used to parse timestamps without an explicit offset.
//
// Returns:
//
//	error: An error if validation fails (e.g., invalid timestamp format or illegal offset range), otherwise nil.
func checkAndRewriteTimestampTzDefaultValueForFieldSchema(
	fieldSchema *schemapb.FieldSchema,
	collectionTimezone string) error {

	defaultValue := fieldSchema.GetDefaultValue()
	if defaultValue == nil {
		return nil
	}
	log.Info("czsKKK111")

	// 2. Read the default value as a string (the initial user-provided format).
	// The default value is expected to be stored in string_data initially.
	stringTz := defaultValue.GetStringData()
	if stringTz == "" {
		// Skip or handle empty string default values if necessary.
		log.Info("czsKKK222")
		return nil
	}

	// 3. Validate the string and convert it to UTC microsecond (int64).
	// The validation function also applies the collectionTimezone if no offset is present
	// in the input stringTz, and performs offset range checks.
	utcMicro, err := timestamptz.ValidateAndReturnUnixMicroTz(stringTz, collectionTimezone)
	if err != nil {
		log.Info("czsKKK333")

		// If validation fails (e.g., invalid format or illegal offset), return error immediately.
		return err
	}

	// 4. Rewrite the default value to store the absolute UTC microsecond (int64).
	// By setting ValueField_LongData, the oneof field in the protobuf structure
	// automatically switches the internal representation from string_data to long_data.
	defaultValue.Data = &schemapb.ValueField_TimestamptzData{
		TimestamptzData: utcMicro,
	}
	fieldSchema.DefaultValue = defaultValue
	log.Info("czsKKK444", zap.Any("utc", fieldSchema.GetDefaultValue()))
	return nil
}
