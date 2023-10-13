#include "storage/stats/rel_table_statistics.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void RelTableStats::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(nextRelOffset);
}

std::unique_ptr<RelTableStats> RelTableStats::deserialize(
    uint64_t numRels, table_id_t tableID, Deserializer& deserializer) {
    offset_t nextRelOffset;
    deserializer.deserializeValue(nextRelOffset);
    return std::make_unique<RelTableStats>(numRels, tableID, nextRelOffset);
}

} // namespace storage
} // namespace kuzu
