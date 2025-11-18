package planparserv2

import (
	"github.com/antlr4-go/antlr/v4"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SchemaHolder defines the interface for any parser object that holds the SchemaHelper.
// This allows the semantic predicate (isTimestamptzCompare) to retrieve the schema
// from the parsing context without knowing the concrete type.
type SchemaHolder interface {
	GetSchemaHelper() *typeutil.SchemaHelper
}

// CustomPlanParser wraps the Antlr-generated PlanParser and adds the necessary dependency.
// This struct should be instantiated in your parsing factory function.
type CustomPlanParser struct {
	*parser.PlanParser
	Schema *typeutil.SchemaHelper
}

// Ensure CustomPlanParser implements SchemaHolder.
func (p *CustomPlanParser) GetSchemaHelper() *typeutil.SchemaHelper {
	return p.Schema
}

// GetSchemaHelperFromParser tries to extract the SchemaHelper from the current Antlr context.
// It checks if the underlying parser object implements the SchemaHolder interface.
func GetSchemaHelperFromParser(parser antlr.Parser) *typeutil.SchemaHelper {
	if holder, ok := parser.(SchemaHolder); ok {
		return holder.GetSchemaHelper()
	}
	return nil
}
