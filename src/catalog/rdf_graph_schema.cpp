#include "catalog/rdf_graph_schema.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void RDFGraphSchema::serialize(common::FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(rdfGraphName, fileInfo, offset);
    SerDeser::serializeValue(rdfGraphID, fileInfo, offset);
    SerDeser::serializeValue(resourcesNodeTableID, fileInfo, offset);
    SerDeser::serializeValue(triplesRelTableID, fileInfo, offset);
}

std::unique_ptr<RDFGraphSchema> RDFGraphSchema::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    std::string rdfGraphName;
    rdf_graph_id_t rdfGraphId;
    table_id_t resourcesNodeTableID, triplesRelTableID;
    SerDeser::deserializeValue(rdfGraphName, fileInfo, offset);
    SerDeser::deserializeValue(rdfGraphId, fileInfo, offset);
    SerDeser::deserializeValue(resourcesNodeTableID, fileInfo, offset);
    SerDeser::deserializeValue(triplesRelTableID, fileInfo, offset);
    return std::make_unique<RDFGraphSchema>(
        rdfGraphName, rdfGraphId, resourcesNodeTableID, triplesRelTableID);
}

} // namespace catalog
} // namespace kuzu