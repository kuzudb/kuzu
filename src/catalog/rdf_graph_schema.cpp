#include "catalog/rdf_graph_schema.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void RdfGraphSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(resourceTableID);
    serializer.serializeValue(literalTableID);
    serializer.serializeValue(resourceTripleTableID);
    serializer.serializeValue(literalTripleTableID);
}

std::unique_ptr<RdfGraphSchema> RdfGraphSchema::deserialize(Deserializer& deserializer) {
    auto schema = std::make_unique<RdfGraphSchema>();
    deserializer.deserializeValue(schema->resourceTableID);
    deserializer.deserializeValue(schema->literalTableID);
    deserializer.deserializeValue(schema->resourceTripleTableID);
    deserializer.deserializeValue(schema->literalTripleTableID);
    return schema;
}

} // namespace catalog
} // namespace kuzu
