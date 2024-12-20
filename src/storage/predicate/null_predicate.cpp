#include "storage/predicate/null_predicate.h"

#include "storage/store/column_chunk_stats.h"

namespace kuzu::storage {
common::ZoneMapCheckResult ColumnNullPredicate::checkZoneMap(
    const MergedColumnChunkStats& mergedStats) const {
    const bool statToCheck = (expressionType == common::ExpressionType::IS_NULL) ?
                                 mergedStats.guaranteedNoNulls :
                                 mergedStats.guaranteedAllNulls;
    return statToCheck ? common::ZoneMapCheckResult::SKIP_SCAN :
                         common::ZoneMapCheckResult::ALWAYS_SCAN;
}

} // namespace kuzu::storage
