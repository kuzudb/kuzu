#include "catalog/rdf_graph_schema.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void RdfGraphSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(nodeTableID);
    serializer.serializeValue(relTableID);
}

std::unique_ptr<RdfGraphSchema> RdfGraphSchema::deserialize(Deserializer& deserializer) {
    table_id_t nodeTableID;
    table_id_t relTableID;
    deserializer.deserializeValue(nodeTableID);
    deserializer.deserializeValue(relTableID);
    return std::make_unique<RdfGraphSchema>(nodeTableID, relTableID);
}

} // namespace catalog
} // namespace kuzu
