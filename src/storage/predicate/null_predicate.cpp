#include "storage/predicate/null_predicate.h"

#include "storage/store/column_chunk_stats.h"

namespace kuzu::storage {
common::ZoneMapCheckResult ColumnIsNullPredicate::checkZoneMap(
    const MergedColumnChunkStats& mergedStats) const {
    return mergedStats.guaranteedNoNulls ? common::ZoneMapCheckResult::SKIP_SCAN :
                                           common::ZoneMapCheckResult::ALWAYS_SCAN;
}

common::ZoneMapCheckResult ColumnIsNotNullPredicate::checkZoneMap(
    const MergedColumnChunkStats& mergedStats) const {
    return mergedStats.guaranteedAllNulls ? common::ZoneMapCheckResult::SKIP_SCAN :
                                            common::ZoneMapCheckResult::ALWAYS_SCAN;
}
} // namespace kuzu::storage
