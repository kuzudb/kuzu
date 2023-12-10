#include "catalog/node_table_schema.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

NodeTableSchema::NodeTableSchema(const NodeTableSchema& other) : TableSchema{other} {
    primaryKeyPropertyID = other.primaryKeyPropertyID;
    fwdRelTableIDSet = other.fwdRelTableIDSet;
    bwdRelTableIDSet = other.bwdRelTableIDSet;
}

void NodeTableSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(primaryKeyPropertyID);
    serializer.serializeUnorderedSet(fwdRelTableIDSet);
    serializer.serializeUnorderedSet(bwdRelTableIDSet);
}

std::unique_ptr<NodeTableSchema> NodeTableSchema::deserialize(Deserializer& deserializer) {
    property_id_t primaryKeyPropertyID;
    std::unordered_set<table_id_t> fwdRelTableIDSet;
    std::unordered_set<table_id_t> bwdRelTableIDSet;
    deserializer.deserializeValue(primaryKeyPropertyID);
    deserializer.deserializeUnorderedSet(fwdRelTableIDSet);
    deserializer.deserializeUnorderedSet(bwdRelTableIDSet);
    auto schema = std::make_unique<NodeTableSchema>();
    schema->primaryKeyPropertyID = primaryKeyPropertyID;
    schema->fwdRelTableIDSet = std::move(fwdRelTableIDSet);
    schema->bwdRelTableIDSet = std::move(bwdRelTableIDSet);
    return schema;
}

} // namespace catalog
} // namespace kuzu
