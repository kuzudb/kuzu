#include "processor/operator/cross_product.h"

namespace kuzu {
namespace processor {

void CrossProduct::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto pos : outVecPos) {
        auto vector = resultSet->getValueVector(pos);
        vectorsToScan.push_back(vector.get());
    }
    startIdx = sharedState->getTable()->getNumTuples();
}

bool CrossProduct::getNextTuplesInternal(ExecutionContext* context) {
    // Note: we should NOT morselize right table scanning (i.e. calling sharedState.getMorsel)
    // because every thread should scan its own table.
    auto table = sharedState->getTable();
    if (table->getNumTuples() == 0) {
        return false;
    }
    if (startIdx == table->getNumTuples()) {       // no more to scan from right
        if (!children[0]->getNextTuple(context)) { // fetch a new left tuple
            return false;
        }
        startIdx = 0; // reset right table scanning for a new left tuple
    }
    // scan from right table if there is tuple left
    auto maxNumTuplesToScan = table->hasUnflatCol() ? 1 : common::DEFAULT_VECTOR_CAPACITY;
    auto numTuplesToScan = std::min(maxNumTuplesToScan, table->getNumTuples() - startIdx);
    table->scan(vectorsToScan, startIdx, numTuplesToScan, colIndicesToScan);
    startIdx += numTuplesToScan;
    metrics->numOutputTuple.increase(numTuplesToScan);
    return true;
}

} // namespace processor
} // namespace kuzu
