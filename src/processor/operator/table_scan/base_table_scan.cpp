#include "processor/operator/table_scan/base_table_scan.h"

namespace kuzu {
namespace processor {

void BaseTableScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& dataPos : outVecPositions) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
}

bool BaseTableScan::getNextTuplesInternal(ExecutionContext* context) {
    auto morsel = getMorsel();
    if (morsel->numTuples == 0) {
        return false;
    }
    morsel->table->scan(vectorsToScan, morsel->startTupleIdx, morsel->numTuples, colIndicesToScan);
    metrics->numOutputTuple.increase(morsel->numTuples);
    return true;
}

} // namespace processor
} // namespace kuzu
