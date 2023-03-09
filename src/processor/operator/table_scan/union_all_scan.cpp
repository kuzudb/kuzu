#include "processor/operator/table_scan/union_all_scan.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

uint64_t UnionAllScanSharedState::getMaxMorselSize() const {
    assert(!fTableSharedStates.empty());
    auto table = fTableSharedStates[0]->getTable();
    return table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
}

std::unique_ptr<FTableScanMorsel> UnionAllScanSharedState::getMorsel(uint64_t maxMorselSize) {
    std::lock_guard<std::mutex> lck{mtx};
    if (fTableToScanIdx == fTableSharedStates.size()) { // No more to scan.
        return std::make_unique<FTableScanMorsel>(nullptr, 0, 0);
    }
    auto morsel = fTableSharedStates[fTableToScanIdx]->getMorsel(maxMorselSize);
    // Fetch next table if current table has nothing to scan.
    while (morsel->numTuples == 0) {
        fTableToScanIdx++;
        if (fTableToScanIdx == fTableSharedStates.size()) { // No more to scan.
            return std::make_unique<FTableScanMorsel>(nullptr, 0, 0);
        }
        morsel = fTableSharedStates[fTableToScanIdx]->getMorsel(maxMorselSize);
    }
    return morsel;
}

} // namespace processor
} // namespace kuzu
