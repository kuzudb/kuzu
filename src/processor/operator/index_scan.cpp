#include "processor/operator/index_scan.h"

#include "common/exception.h"

namespace kuzu {
namespace processor {

void IndexScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    outVector = resultSet->getValueVector(outDataPos);
    indexKeyEvaluator->init(*resultSet, context->memoryManager);
    hasExecuted = false;
}

bool IndexScan::getNextTuplesInternal() {
    if (hasExecuted) {
        return false;
    }
    indexKeyEvaluator->evaluate();
    auto indexKeyVector = indexKeyEvaluator->resultVector.get();
    assert(indexKeyVector->state->isFlat());
    offset_t nodeOffset;
    bool isSuccessfulLookup = pkIndex->lookup(transaction, indexKeyVector,
        indexKeyVector->state->selVector->selectedPositions[0], nodeOffset);
    if (isSuccessfulLookup) {
        hasExecuted = true;
        nodeID_t nodeID{nodeOffset, tableID};
        outVector->setValue<nodeID_t>(0, nodeID);
    }
    return isSuccessfulLookup;
}

} // namespace processor
} // namespace kuzu
