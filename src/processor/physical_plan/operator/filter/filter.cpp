#include "src/processor/include/physical_plan/operator/filter/filter.h"

#include "src/processor/include/physical_plan/expression_mapper.h"

namespace graphflow {
namespace processor {

Filter::Filter(unique_ptr<ExpressionEvaluator> rootExpr, uint64_t dataChunkToSelectPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), FILTER, context, id},
      FilteringOperator(this->prevOperator->getResultSet()->dataChunks[dataChunkToSelectPos]),
      rootExpr{move(rootExpr)}, dataChunkToSelectPos(dataChunkToSelectPos) {
    resultSet = this->prevOperator->getResultSet();
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
}

void Filter::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneSelectedValue;
    do {
        restoreDataChunkSelectorState();
        prevOperator->getNextTuples();
        if (dataChunkToSelect->state->selectedSize == 0) {
            break;
        }
        saveDataChunkSelectorState();
        if (dataChunkToSelect->state->isFlat()) {
            hasAtLeastOneSelectedValue =
                rootExpr->select(dataChunkToSelect->state->selectedPositions) > 0;
        } else {
            dataChunkToSelect->state->selectedSize =
                rootExpr->select(dataChunkToSelect->state->selectedPositionsBuffer.get());
            if (dataChunkToSelect->state->isUnfiltered()) {
                dataChunkToSelect->state->resetSelectorToValuePosBuffer();
            }
            hasAtLeastOneSelectedValue = dataChunkToSelect->state->selectedSize > 0;
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(dataChunkToSelect->state->selectedSize);
}

unique_ptr<PhysicalOperator> Filter::clone() {
    auto prevOperatorClone = prevOperator->clone();
    auto rootExprClone = ExpressionMapper::clone(*context.memoryManager, *rootExpr,
        *prevOperatorClone->getResultSet(), true /* isSelectOperation */);
    return make_unique<Filter>(
        move(rootExprClone), dataChunkToSelectPos, move(prevOperatorClone), context, id);
}

} // namespace processor
} // namespace graphflow
