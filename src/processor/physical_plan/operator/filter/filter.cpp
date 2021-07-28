#include "src/processor/include/physical_plan/operator/filter/filter.h"

#include "src/processor/include/physical_plan/expression_mapper.h"

namespace graphflow {
namespace processor {

Filter::Filter(unique_ptr<ExpressionEvaluator> rootExpr, uint64_t dataChunkToSelectPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), FILTER, context, id}, rootExpr{move(rootExpr)},
      FilteringOperator(this->prevOperator->getResultSet()->dataChunks[dataChunkToSelectPos]),
      dataChunkToSelectPos(dataChunkToSelectPos) {
    resultSet = this->prevOperator->getResultSet();
    exprResultValues = this->rootExpr->result->values;
}

void Filter::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneSelectedValue;
    do {
        restoreDataChunkSelectorState();
        prevOperator->getNextTuples();
        if (dataChunkToSelect->state->size == 0) {
            break;
        }
        saveDataChunkSelectorState();
        rootExpr->evaluate();
        if (dataChunkToSelect->state->isFlat()) {
            auto pos = dataChunkToSelect->state->getPositionOfCurrIdx();
            hasAtLeastOneSelectedValue =
                (exprResultValues[pos] == TRUE) & (!rootExpr->result->isNull(pos));
        } else {
            auto resultPos = 0;
            if (dataChunkToSelect->state->isUnfiltered()) {
                dataChunkToSelect->state->resetSelectorToValuePosBuffer();
                for (auto i = 0ul; i < dataChunkToSelect->state->size; i++) {
                    dataChunkToSelect->state->selectedPositions[resultPos] = i;
                    resultPos += (exprResultValues[i] == TRUE) & (!rootExpr->result->isNull(i));
                }
            } else {
                for (auto i = 0ul; i < dataChunkToSelect->state->size; i++) {
                    auto pos = dataChunkToSelect->state->selectedPositions[i];
                    dataChunkToSelect->state->selectedPositions[resultPos] = pos;
                    resultPos += (exprResultValues[pos] == TRUE) & (!rootExpr->result->isNull(pos));
                }
            }
            dataChunkToSelect->state->size = resultPos;
            hasAtLeastOneSelectedValue = resultPos > 0;
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(dataChunkToSelect->state->size);
}

unique_ptr<PhysicalOperator> Filter::clone() {
    auto prevOperatorClone = prevOperator->clone();
    auto rootExprClone = ExpressionMapper::clone(
        *context.memoryManager, *rootExpr, *prevOperatorClone->getResultSet());
    return make_unique<Filter>(
        move(rootExprClone), dataChunkToSelectPos, move(prevOperatorClone), context, id);
}

} // namespace processor
} // namespace graphflow
