#include "src/processor/include/physical_plan/operator/filter/filter.h"

namespace graphflow {
namespace processor {

Filter::Filter(unique_ptr<PhysicalExpression> rootExpr, uint64_t dataChunkToSelectPos,
    unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), FILTER}, rootExpr{move(rootExpr)},
      dataChunkToSelectPos(dataChunkToSelectPos) {
    dataChunks = this->prevOperator->getDataChunks();
    dataChunkToSelect = dataChunks->getDataChunk(dataChunkToSelectPos);
    this->rootExpr->setExpressionInputDataChunk(dataChunkToSelect);
    this->rootExpr->setExpressionResultOwnerState(dataChunkToSelect->state);
    exprResult = this->rootExpr->result->getValues();
}

void Filter::getNextTuples() {
    do {
        prevOperator->getNextTuples();
        if (dataChunkToSelect->state->numSelectedValues > 0) {
            if (dataChunkToSelect->isFlat()) {
                // not currently used due to the enumerator filter push down.
            } else {
                rootExpr->evaluate();
                auto sizeFiltered = dataChunkToSelect->state->numSelectedValues;
                auto writePos = 0;
                for (auto i = 0ul; i < sizeFiltered; i++) {
                    auto pos = dataChunkToSelect->state->selectedValuesPos[i];
                    if (exprResult[pos] == TRUE) {
                        dataChunkToSelect->state->selectedValuesPos[writePos++] = pos;
                    }
                }
                dataChunkToSelect->state->numSelectedValues = writePos;
            }
        }
    } while (
        dataChunkToSelect->state->size > 0 && dataChunkToSelect->state->numSelectedValues == 0);
}

unique_ptr<PhysicalOperator> Filter::clone() {
    auto rootExprClone = ExpressionMapper::clone(*rootExpr);
    return make_unique<Filter>(move(rootExprClone), dataChunkToSelectPos, prevOperator->clone());
}

} // namespace processor
} // namespace graphflow
