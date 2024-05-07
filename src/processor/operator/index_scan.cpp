#include "processor/operator/index_scan.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void IndexScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    indexEvaluator->init(*resultSet, context->clientContext->getMemoryManager());
    indexVector = indexEvaluator->resultVector.get();
    outVector = resultSet->getValueVector(outDataPos).get();
}

bool IndexScan::getNextTuplesInternal(ExecutionContext* context) {
    auto numSelectedValues = 0u;
    do {
        restoreSelVector(*outVector->state);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(*outVector->state);
        numSelectedValues = 0u;
        auto buffer = outVector->state->getSelVectorUnsafe().getMultableBuffer();
        for (auto i = 0u; i < indexVector->state->getSelVector().getSelSize(); ++i) {
            auto pos = indexVector->state->getSelVector()[i];
            if (indexVector->isNull(pos)) {
                continue;
            }
            buffer[numSelectedValues] = pos;
            offset_t nodeOffset = INVALID_OFFSET;
            numSelectedValues +=
                pkIndex->lookup(context->clientContext->getTx(), indexVector, pos, nodeOffset);
            nodeID_t nodeID{nodeOffset, tableID};
            outVector->setValue<nodeID_t>(pos, nodeID);
        }
        if (!outVector->state->isFlat() && outVector->state->getSelVector().isUnfiltered()) {
            outVector->state->getSelVectorUnsafe().setToFiltered();
        }
    } while (numSelectedValues == 0);
    outVector->state->getSelVectorUnsafe().setSelSize(numSelectedValues);
    metrics->numOutputTuple.increase(numSelectedValues);
    return true;
}

} // namespace processor
} // namespace kuzu
