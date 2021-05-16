#include "src/processor/include/physical_plan/operator/filter/filter.h"

namespace graphflow {
namespace processor {

template<bool IS_AFTER_FLATTEN>
Filter<IS_AFTER_FLATTEN>::Filter(unique_ptr<ExpressionEvaluator> rootExpr,
    uint64_t dataChunkToSelectPos, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), FILTER}, rootExpr{move(rootExpr)},
      dataChunkToSelectPos(dataChunkToSelectPos), prevInNumSelectedValues{0ul} {
    if (IS_AFTER_FLATTEN) {
        prevInSelectedValuesPos = make_unique<uint64_t[]>(MAX_VECTOR_SIZE);
    }
    resultSet = this->prevOperator->getResultSet();
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
    exprResult = this->rootExpr->result->values;
}

template<bool IS_AFTER_FLATTEN>
void Filter<IS_AFTER_FLATTEN>::getNextTuples() {
    bool hasAtLeastOneSelectedValue;
    do {
        if (IS_AFTER_FLATTEN) {
            restoreDataChunkToSelectState();
        }
        prevOperator->getNextTuples();
        if (dataChunkToSelect->state->numSelectedValues > 0) {
            if (IS_AFTER_FLATTEN) {
                saveDataChunkToSelectState();
            }
            rootExpr->evaluate();
            if (dataChunkToSelect->state->isFlat()) {
                hasAtLeastOneSelectedValue =
                    exprResult[dataChunkToSelect->state->getCurrSelectedValuesPos()] == TRUE;
            } else {
                auto resultPos = 0;
                for (auto i = 0ul; i < dataChunkToSelect->state->numSelectedValues; i++) {
                    auto pos = dataChunkToSelect->state->selectedValuesPos[i];
                    if (exprResult[pos] == TRUE) {
                        dataChunkToSelect->state->selectedValuesPos[resultPos++] = pos;
                    }
                }
                dataChunkToSelect->state->numSelectedValues = resultPos;
                hasAtLeastOneSelectedValue = resultPos > 0;
            }
        }
    } while (dataChunkToSelect->state->size > 0 && !hasAtLeastOneSelectedValue);
}

template<bool IS_AFTER_FLATTEN>
unique_ptr<PhysicalOperator> Filter<IS_AFTER_FLATTEN>::clone() {
    auto prevOperatorClone = prevOperator->clone();
    auto rootExprClone = ExpressionMapper::clone(*rootExpr, *prevOperatorClone->getResultSet());
    return make_unique<Filter<IS_AFTER_FLATTEN>>(
        move(rootExprClone), dataChunkToSelectPos, move(prevOperatorClone));
}

template<bool IS_AFTER_FLATTEN>
void Filter<IS_AFTER_FLATTEN>::restoreDataChunkToSelectState() {
    dataChunkToSelect->state->numSelectedValues = prevInNumSelectedValues;
    std::copy(prevInSelectedValuesPos.get(),
        prevInSelectedValuesPos.get() + prevInNumSelectedValues,
        dataChunkToSelect->state->selectedValuesPos);
}

template<bool IS_AFTER_FLATTEN>
void Filter<IS_AFTER_FLATTEN>::saveDataChunkToSelectState() {
    prevInNumSelectedValues = dataChunkToSelect->state->numSelectedValues;
    std::copy(dataChunkToSelect->state->selectedValuesPos,
        dataChunkToSelect->state->selectedValuesPos + prevInNumSelectedValues,
        prevInSelectedValuesPos.get());
}

template class Filter<true>;
template class Filter<false>;
} // namespace processor
} // namespace graphflow
