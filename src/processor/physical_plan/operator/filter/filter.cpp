#include "src/processor/include/physical_plan/operator/filter/filter.h"

namespace graphflow {
namespace processor {

Filter::Filter(unique_ptr<PhysicalExpression> rootExpr, uint64_t dataChunkToSelectPos,
    unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), FILTER}, rootExpr{move(rootExpr)},
      dataChunkToSelectPos(dataChunkToSelectPos) {
    dataChunks = this->prevOperator->getDataChunks();
    dataChunkToSelect = dataChunks->getDataChunk(dataChunkToSelectPos);
    this->rootExpr->setExpressionResultOwners(dataChunkToSelect);
    exprResult = this->rootExpr->result->getValues();
}

void Filter::getNextTuples() {
    do {
        prevOperator->getNextTuples();
        if (dataChunkToSelect->numSelectedValues > 0) {
            if (dataChunkToSelect->isFlat()) {
                // not currently used due to the enumerator filter push down.
            } else {
                rootExpr->evaluate();
                auto sizeFiltered = dataChunkToSelect->numSelectedValues;
                auto writePos = 0;
                for (auto i = 0ul; i < sizeFiltered; i++) {
                    auto pos = dataChunkToSelect->selectedValuesPos[i];
                    if (exprResult[pos] == TRUE) {
                        dataChunkToSelect->selectedValuesPos[writePos++] = pos;
                    }
                }
                dataChunkToSelect->numSelectedValues = writePos;
            }
        }
    } while (dataChunkToSelect->size > 0 && dataChunkToSelect->numSelectedValues == 0);
}

unique_ptr<PhysicalOperator> Filter::clone() {
    auto prevOperatorClone = prevOperator->clone();
    auto rootExprClone = ExpressionMapper::clone(*rootExpr, *prevOperatorClone->getDataChunks());
    return make_unique<Filter>(move(rootExprClone), dataChunkToSelectPos, move(prevOperatorClone));
}

} // namespace processor
} // namespace graphflow
