#include "src/processor/include/physical_plan/operator/filter/filter.h"

namespace graphflow {
namespace processor {

Filter::Filter(unique_ptr<PhysicalExpression> rootExpr, uint64_t dataChunkToSelectPos,
    unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator)}, rootExpr{move(rootExpr)},
      dataChunkToSelectPos(dataChunkToSelectPos) {
    dataChunks = this->prevOperator->getDataChunks();
    dataChunkToSelect = dataChunks->getDataChunk(dataChunkToSelectPos);
    exprResult = this->rootExpr->result->getValues();
}

void Filter::getNextTuples() {
    prevOperator->getNextTuples();
    if (dataChunkToSelect->numSelectedValues > 0) {
        rootExpr->evaluate();
        auto sizeFiltered = dataChunkToSelect->numSelectedValues;
        auto pos = 0;
        for (auto i = 0ul; i < sizeFiltered; i++) {
            if (exprResult[i] == TRUE) {
                dataChunkToSelect->selectedValuesPos[pos++] =
                    dataChunkToSelect->selectedValuesPos[i];
            }
        }
        dataChunkToSelect->numSelectedValues = pos;
    }
}

} // namespace processor
} // namespace graphflow
