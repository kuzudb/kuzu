#include "expression_evaluator/case_evaluator.h"

#include <cmath>

#include "common/types/date_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_string.h"
#include "common/types/timestamp_t.h"
#include "common/types/types.h"

using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

void CaseAlternativeEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    whenEvaluator->init(resultSet, memoryManager);
    thenEvaluator->init(resultSet, memoryManager);
    whenSelVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    whenSelVector->resetSelectorToValuePosBuffer();
}

void CaseExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    for (auto& alternativeEvaluator : alternativeEvaluators) {
        alternativeEvaluator->init(resultSet, memoryManager);
    }
    elseEvaluator->init(resultSet, memoryManager);
    ExpressionEvaluator::init(resultSet, memoryManager);
}

void CaseExpressionEvaluator::evaluate() {
    filledMask.reset();
    for (auto& alternativeEvaluator : alternativeEvaluators) {
        auto whenSelVector = alternativeEvaluator->whenSelVector.get();
        auto hasAtLeastOneValue = alternativeEvaluator->whenEvaluator->select(*whenSelVector);
        if (!hasAtLeastOneValue) {
            continue;
        }
        alternativeEvaluator->thenEvaluator->evaluate();
        auto thenVector = alternativeEvaluator->thenEvaluator->resultVector.get();
        if (alternativeEvaluator->whenEvaluator->isResultFlat()) {
            fillAllSwitch(*thenVector);
        } else {
            fillSelectedSwitch(*whenSelVector, *thenVector);
        }
        if (filledMask.count() == resultVector->state->selVector->selectedSize) {
            return;
        }
    }
    elseEvaluator->evaluate();
    fillAllSwitch(*elseEvaluator->resultVector);
}

bool CaseExpressionEvaluator::select(SelectionVector& selVector) {
    evaluate();
    auto numSelectedValues = 0u;
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; ++i) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        auto selectedPosBuffer = selVector.getSelectedPositionsBuffer();
        selectedPosBuffer[numSelectedValues] = pos;
        numSelectedValues += resultVector->getValue<bool>(pos);
    }
    selVector.selectedSize = numSelectedValues;
    return numSelectedValues > 0;
}

std::unique_ptr<ExpressionEvaluator> CaseExpressionEvaluator::clone() {
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> clonedAlternativeEvaluators;
    for (auto& alternative : alternativeEvaluators) {
        clonedAlternativeEvaluators.push_back(alternative->clone());
    }
    return make_unique<CaseExpressionEvaluator>(
        expression, std::move(clonedAlternativeEvaluators), std::move(elseEvaluator->clone()));
}

void CaseExpressionEvaluator::resolveResultVector(
    const ResultSet& /*resultSet*/, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->dataType, memoryManager);
    std::vector<ExpressionEvaluator*> inputEvaluators;
    for (auto& alternative : alternativeEvaluators) {
        inputEvaluators.push_back(alternative->whenEvaluator.get());
        inputEvaluators.push_back(alternative->thenEvaluator.get());
    }
    inputEvaluators.push_back(elseEvaluator.get());
    resolveResultStateFromChildren(inputEvaluators);
}

template<typename T>
void CaseExpressionEvaluator::fillEntry(sel_t resultPos, const ValueVector& thenVector) {
    if (filledMask[resultPos]) {
        return;
    }
    filledMask[resultPos] = true;
    auto thenPos =
        thenVector.state->isFlat() ? thenVector.state->selVector->selectedPositions[0] : resultPos;
    if (thenVector.isNull(thenPos)) {
        resultVector->setNull(resultPos, true);
    } else {
        if (thenVector.dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
            auto srcListEntry = thenVector.getValue<list_entry_t>(thenPos);
            list_entry_t resultEntry = ListVector::addList(resultVector.get(), srcListEntry.size);
            resultVector->copyFromVectorData(resultPos, &thenVector, thenPos);
        } else {
            auto val = thenVector.getValue<T>(thenPos);
            resultVector->setValue<T>(resultPos, val);
        }
    }
}

void CaseExpressionEvaluator::fillAllSwitch(const ValueVector& thenVector) {
    switch (resultVector->dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        fillAll<bool>(thenVector);
    } break;
    case LogicalTypeID::INT64: {
        fillAll<int64_t>(thenVector);
    } break;
    case LogicalTypeID::DOUBLE: {
        fillAll<double_t>(thenVector);
    } break;
    case LogicalTypeID::DATE: {
        fillAll<date_t>(thenVector);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        fillAll<timestamp_t>(thenVector);
    } break;
    case LogicalTypeID::INTERVAL: {
        fillAll<interval_t>(thenVector);
    } break;
    case LogicalTypeID::STRING: {
        fillAll<ku_string_t>(thenVector);
    } break;
    case LogicalTypeID::VAR_LIST: {
        fillAll<list_entry_t>(thenVector);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void CaseExpressionEvaluator::fillSelectedSwitch(
    const SelectionVector& selVector, const ValueVector& thenVector) {
    switch (resultVector->dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        fillSelected<bool>(selVector, thenVector);
    } break;
    case LogicalTypeID::INT64: {
        fillSelected<int64_t>(selVector, thenVector);
    } break;
    case LogicalTypeID::DOUBLE: {
        fillSelected<double_t>(selVector, thenVector);
    } break;
    case LogicalTypeID::DATE: {
        fillSelected<date_t>(selVector, thenVector);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        fillSelected<timestamp_t>(selVector, thenVector);
    } break;
    case LogicalTypeID::INTERVAL: {
        fillSelected<interval_t>(selVector, thenVector);
    } break;
    case LogicalTypeID::STRING: {
        fillSelected<ku_string_t>(selVector, thenVector);
    } break;
    case LogicalTypeID::VAR_LIST: {
        fillSelected<list_entry_t>(selVector, thenVector);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace evaluator
} // namespace kuzu
