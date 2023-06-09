#include "processor/operator/table_scan/union_all_scan.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<FTableScanMorsel> UnionAllScanSharedState::getMorsel() {
    std::lock_guard<std::mutex> lck{mtx};
    if (fTableToScanIdx == fTableSharedStates.size()) { // No more to scan.
        return std::make_unique<FTableScanMorsel>(nullptr, 0, 0);
    }
    auto morsel = fTableSharedStates[fTableToScanIdx]->getMorsel();
    // Fetch next table if current table has nothing to scan.
    while (morsel->numTuples == 0) {
        fTableToScanIdx++;
        if (fTableToScanIdx == fTableSharedStates.size()) { // No more to scan.
            return std::make_unique<FTableScanMorsel>(nullptr, 0, 0);
        }
        morsel = fTableSharedStates[fTableToScanIdx]->getMorsel();
    }
    return morsel;
}

} // namespace processor
} // namespace kuzu
