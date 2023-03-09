#include "processor/operator/index_scan.h"

#include "common/exception.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void IndexScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    assert(indexDataPos.dataChunkPos == outDataPos.dataChunkPos);
    indexVector = resultSet->getValueVector(indexDataPos);
    outVector = resultSet->getValueVector(outDataPos);
}

bool IndexScan::getNextTuplesInternal() {
    auto numSelectedValues = 0u;
    do {
        restoreSelVector(outVector->state->selVector);
        if (!children[0]->getNextTuple()) {
            return false;
        }
        saveSelVector(outVector->state->selVector);
        numSelectedValues = 0u;
        for (auto i = 0; i < indexVector->state->selVector->selectedSize; ++i) {
            auto pos = indexVector->state->selVector->selectedPositions[i];
            outVector->state->selVector->getSelectedPositionsBuffer()[numSelectedValues] = pos;
            offset_t nodeOffset = INVALID_NODE_OFFSET;
            numSelectedValues += pkIndex->lookup(transaction, indexVector.get(), pos, nodeOffset);
            nodeID_t nodeID{nodeOffset, tableID};
            outVector->setValue<nodeID_t>(pos, nodeID);
        }
        if (!outVector->state->isFlat() && outVector->state->selVector->isUnfiltered()) {
            outVector->state->selVector->resetSelectorToValuePosBuffer();
        }
    } while (numSelectedValues == 0);
    outVector->state->selVector->selectedSize = numSelectedValues;
    metrics->numOutputTuple.increase(numSelectedValues);
    return true;
}

} // namespace processor
} // namespace kuzu
