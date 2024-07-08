#include "parser/transformer.h"
#include "parser/update_vector_index.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformUpdateVectorIndex(
    CypherParser::KU_UpdateVectorIndexContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyName = transformPropertyKeyName(*ctx.oC_PropertyKeyName());
    return std::make_unique<UpdateVectorIndex>(tableName, propertyName);
}

} // namespace parser
} // namespace kuzu
