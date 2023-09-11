#include "catalog/rdf_graph_schema.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void RdfGraphSchema::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(nodeTableID, fileInfo, offset);
    SerDeser::serializeValue(relTableID, fileInfo, offset);
}

std::unique_ptr<RdfGraphSchema> RdfGraphSchema::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    table_id_t nodeTableID;
    table_id_t relTableID;
    SerDeser::deserializeValue(nodeTableID, fileInfo, offset);
    SerDeser::deserializeValue(relTableID, fileInfo, offset);
    return std::make_unique<RdfGraphSchema>(nodeTableID, relTableID);
}

} // namespace catalog
} // namespace kuzu
