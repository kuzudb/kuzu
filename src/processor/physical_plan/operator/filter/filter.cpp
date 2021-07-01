#include "src/processor/include/physical_plan/operator/filter/filter.h"

#include "src/processor/include/physical_plan/expression_mapper.h"

namespace graphflow {
namespace processor {

template<bool IS_AFTER_FLATTEN>
Filter<IS_AFTER_FLATTEN>::Filter(unique_ptr<ExpressionEvaluator> rootExpr,
    uint64_t dataChunkToSelectPos, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), FILTER, context, id}, rootExpr{move(rootExpr)},
      dataChunkToSelectPos(dataChunkToSelectPos), prevInNumSelectedValues{0ul} {
    if (IS_AFTER_FLATTEN) {
        prevInSelectedValuesPos = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    }
    resultSet = this->prevOperator->getResultSet();
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
    exprResultValues = this->rootExpr->result->values;
    exprResultNullMask = this->rootExpr->result->nullMask;
}

template<bool IS_AFTER_FLATTEN>
void Filter<IS_AFTER_FLATTEN>::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneSelectedValue;
    do {
        if (IS_AFTER_FLATTEN) {
            restoreDataChunkToSelectState();
        }
        prevOperator->getNextTuples();
        if (dataChunkToSelect->state->size == 0) {
            break;
        }
        if (IS_AFTER_FLATTEN) {
            saveDataChunkToSelectState();
        }
        rootExpr->evaluate();
        if (dataChunkToSelect->state->isFlat()) {
            auto pos = dataChunkToSelect->state->getCurrSelectedValuesPos();
            hasAtLeastOneSelectedValue =
                (exprResultValues[pos] == TRUE) & (!exprResultNullMask[pos]);
        } else {
            auto resultPos = 0;
            for (auto i = 0ul; i < dataChunkToSelect->state->size; i++) {
                auto pos = dataChunkToSelect->state->selectedValuesPos[i];
                dataChunkToSelect->state->selectedValuesPos[resultPos] = pos;
                resultPos += (exprResultValues[pos] == TRUE) & (!exprResultNullMask[pos]);
            }
            dataChunkToSelect->state->size = resultPos;
            hasAtLeastOneSelectedValue = resultPos > 0;
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(dataChunkToSelect->state->size);
}

template<bool IS_AFTER_FLATTEN>
unique_ptr<PhysicalOperator> Filter<IS_AFTER_FLATTEN>::clone() {
    auto prevOperatorClone = prevOperator->clone();
    auto rootExprClone = ExpressionMapper::clone(
        *context.memoryManager, *rootExpr, *prevOperatorClone->getResultSet());
    return make_unique<Filter<IS_AFTER_FLATTEN>>(
        move(rootExprClone), dataChunkToSelectPos, move(prevOperatorClone), context, id);
}

template<bool IS_AFTER_FLATTEN>
void Filter<IS_AFTER_FLATTEN>::restoreDataChunkToSelectState() {
    dataChunkToSelect->state->size = prevInNumSelectedValues;
    std::copy(prevInSelectedValuesPos.get(),
        prevInSelectedValuesPos.get() + prevInNumSelectedValues,
        dataChunkToSelect->state->selectedValuesPos);
}

template<bool IS_AFTER_FLATTEN>
void Filter<IS_AFTER_FLATTEN>::saveDataChunkToSelectState() {
    prevInNumSelectedValues = dataChunkToSelect->state->size;
    std::copy(dataChunkToSelect->state->selectedValuesPos,
        dataChunkToSelect->state->selectedValuesPos + prevInNumSelectedValues,
        prevInSelectedValuesPos.get());
}

template class Filter<true>;
template class Filter<false>;
} // namespace processor
} // namespace graphflow
