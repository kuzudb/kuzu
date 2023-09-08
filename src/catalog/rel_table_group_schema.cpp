#include "catalog/rel_table_group_schema.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void RelTableGroupSchema::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeVector(relTableIDs, fileInfo, offset);
}

std::unique_ptr<RelTableGroupSchema> RelTableGroupSchema::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    std::vector<table_id_t> relTableIDs;
    SerDeser::deserializeVector(relTableIDs, fileInfo, offset);
    return std::make_unique<RelTableGroupSchema>(std::move(relTableIDs));
}

} // namespace catalog
} // namespace kuzu
