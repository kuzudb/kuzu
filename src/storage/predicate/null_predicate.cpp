#include "storage/predicate/null_predicate.h"

#include "storage/store/column_chunk_stats.h"

namespace kuzu::storage {
common::ZoneMapCheckResult ColumnIsNullPredicate::checkZoneMap(
    const ColumnChunkStats& stats) const {
    return stats.mayHaveNulls ? common::ZoneMapCheckResult::ALWAYS_SCAN :
                                common::ZoneMapCheckResult::SKIP_SCAN;
}

common::ZoneMapCheckResult ColumnIsNotNullPredicate::checkZoneMap(const ColumnChunkStats&) const {
    return common::ZoneMapCheckResult::ALWAYS_SCAN;
}
} // namespace kuzu::storage
