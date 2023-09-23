#include "storage/stats/rel_table_statistics.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void RelTableStats::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(nextRelOffset, fileInfo, offset);
}

std::unique_ptr<RelTableStats> RelTableStats::deserialize(
    uint64_t numRels, common::table_id_t tableID, FileInfo* fileInfo, uint64_t& offset) {
    common::offset_t nextRelOffset;
    SerDeser::deserializeValue(nextRelOffset, fileInfo, offset);
    return std::make_unique<RelTableStats>(numRels, tableID, nextRelOffset);
}

} // namespace storage
} // namespace kuzu
