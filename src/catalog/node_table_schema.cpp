#include "catalog/node_table_schema.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void NodeTableSchema::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(primaryKeyPropertyID, fileInfo, offset);
    SerDeser::serializeUnorderedSet(fwdRelTableIDSet, fileInfo, offset);
    SerDeser::serializeUnorderedSet(bwdRelTableIDSet, fileInfo, offset);
}

std::unique_ptr<NodeTableSchema> NodeTableSchema::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    property_id_t primaryKeyPropertyID;
    std::unordered_set<table_id_t> fwdRelTableIDSet;
    std::unordered_set<table_id_t> bwdRelTableIDSet;
    SerDeser::deserializeValue(primaryKeyPropertyID, fileInfo, offset);
    SerDeser::deserializeUnorderedSet(fwdRelTableIDSet, fileInfo, offset);
    SerDeser::deserializeUnorderedSet(bwdRelTableIDSet, fileInfo, offset);
    return std::make_unique<NodeTableSchema>(
        primaryKeyPropertyID, fwdRelTableIDSet, bwdRelTableIDSet);
}

} // namespace catalog
} // namespace kuzu
