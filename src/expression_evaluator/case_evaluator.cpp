#include "expression_evaluator/case_evaluator.h"

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
    BaseExpressionEvaluator::init(resultSet, memoryManager);
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

std::unique_ptr<BaseExpressionEvaluator> CaseExpressionEvaluator::clone() {
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> clonedAlternativeEvaluators;
    for (auto& alternative : alternativeEvaluators) {
        clonedAlternativeEvaluators.push_back(alternative->clone());
    }
    return make_unique<CaseExpressionEvaluator>(
        expression, std::move(clonedAlternativeEvaluators), std::move(elseEvaluator->clone()));
}

void CaseExpressionEvaluator::resolveResultVector(
    const ResultSet& resultSet, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->dataType, memoryManager);
    std::vector<BaseExpressionEvaluator*> inputEvaluators;
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
        auto val = thenVector.getValue<T>(thenPos);
        resultVector->setValue<T>(resultPos, val);
    }
}

void CaseExpressionEvaluator::fillAllSwitch(const ValueVector& thenVector) {
    auto typeID = resultVector->dataType.typeID;
    switch (typeID) {
    case BOOL: {
        fillAll<bool>(thenVector);
    } break;
    case INT64: {
        fillAll<int64_t>(thenVector);
    } break;
    case DOUBLE: {
        fillAll<double_t>(thenVector);
    } break;
    case DATE: {
        fillAll<date_t>(thenVector);
    } break;
    case TIMESTAMP: {
        fillAll<timestamp_t>(thenVector);
    } break;
    case INTERVAL: {
        fillAll<interval_t>(thenVector);
    } break;
    case STRING: {
        fillAll<ku_string_t>(thenVector);
    } break;
    case VAR_LIST: {
        fillAll<ku_list_t>(thenVector);
    } break;
    default:
        throw NotImplementedException(
            "Unimplemented type " + Types::dataTypeToString(typeID) + " for case expression.");
    }
}

void CaseExpressionEvaluator::fillSelectedSwitch(
    const SelectionVector& selVector, const ValueVector& thenVector) {
    auto typeID = resultVector->dataType.typeID;
    switch (typeID) {
    case BOOL: {
        fillSelected<bool>(selVector, thenVector);
    } break;
    case INT64: {
        fillSelected<int64_t>(selVector, thenVector);
    } break;
    case DOUBLE: {
        fillSelected<double_t>(selVector, thenVector);
    } break;
    case DATE: {
        fillSelected<date_t>(selVector, thenVector);
    } break;
    case TIMESTAMP: {
        fillSelected<timestamp_t>(selVector, thenVector);
    } break;
    case INTERVAL: {
        fillSelected<interval_t>(selVector, thenVector);
    } break;
    case STRING: {
        fillSelected<ku_string_t>(selVector, thenVector);
    } break;
    case VAR_LIST: {
        fillSelected<ku_list_t>(selVector, thenVector);
    } break;
    default:
        throw NotImplementedException(
            "Unimplemented type " + Types::dataTypeToString(typeID) + " for case expression.");
    }
}

} // namespace evaluator
} // namespace kuzu
