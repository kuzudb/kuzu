#include "expression_evaluator/case_evaluator.h"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

void CaseAlternativeEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    whenEvaluator->init(resultSet, memoryManager);
    thenEvaluator->init(resultSet, memoryManager);
    whenSelVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    whenSelVector->setToFiltered();
}

void CaseExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    for (auto& alternativeEvaluator : alternativeEvaluators) {
        alternativeEvaluator->init(resultSet, memoryManager);
    }
    elseEvaluator->init(resultSet, memoryManager);
    ExpressionEvaluator::init(resultSet, memoryManager);
}

void CaseExpressionEvaluator::evaluate(ClientContext* clientContext) {
    filledMask.reset();
    for (auto& alternativeEvaluator : alternativeEvaluators) {
        auto whenSelVector = alternativeEvaluator->whenSelVector.get();
        auto hasAtLeastOneValue =
            alternativeEvaluator->whenEvaluator->select(*whenSelVector, clientContext);
        if (!hasAtLeastOneValue) {
            continue;
        }
        alternativeEvaluator->thenEvaluator->evaluate(clientContext);
        auto thenVector = alternativeEvaluator->thenEvaluator->resultVector.get();
        if (alternativeEvaluator->whenEvaluator->isResultFlat()) {
            fillAll(thenVector);
        } else {
            fillSelected(*whenSelVector, thenVector);
        }
        if (filledMask.count() == resultVector->state->selVector->selectedSize) {
            return;
        }
    }
    elseEvaluator->evaluate(clientContext);
    fillAll(elseEvaluator->resultVector.get());
}

bool CaseExpressionEvaluator::select(SelectionVector& selVector, ClientContext* clientContext) {
    evaluate(clientContext);
    KU_ASSERT(resultVector->state->selVector->selectedSize == selVector.selectedSize);
    auto numSelectedValues = 0u;
    auto selectedPosBuffer = selVector.getMultableBuffer();
    for (auto i = 0u; i < selVector.selectedSize; ++i) {
        auto selVectorPos = selVector.selectedPositions[i];
        auto resultVectorPos = resultVector->state->selVector->selectedPositions[i];
        selectedPosBuffer[numSelectedValues] = selVectorPos;
        numSelectedValues += resultVector->getValue<bool>(resultVectorPos);
    }
    selVector.selectedSize = numSelectedValues;
    return numSelectedValues > 0;
}

std::unique_ptr<ExpressionEvaluator> CaseExpressionEvaluator::clone() {
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> clonedAlternativeEvaluators;
    clonedAlternativeEvaluators.reserve(alternativeEvaluators.size());
    for (auto& alternative : alternativeEvaluators) {
        clonedAlternativeEvaluators.push_back(alternative->clone());
    }
    return make_unique<CaseExpressionEvaluator>(expression, std::move(clonedAlternativeEvaluators),
        elseEvaluator->clone());
}

void CaseExpressionEvaluator::resolveResultVector(const ResultSet& /*resultSet*/,
    MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->dataType, memoryManager);
    std::vector<ExpressionEvaluator*> inputEvaluators;
    for (auto& alternative : alternativeEvaluators) {
        inputEvaluators.push_back(alternative->whenEvaluator.get());
        inputEvaluators.push_back(alternative->thenEvaluator.get());
    }
    inputEvaluators.push_back(elseEvaluator.get());
    resolveResultStateFromChildren(inputEvaluators);
}

void CaseExpressionEvaluator::fillSelected(const SelectionVector& selVector,
    ValueVector* srcVector) {
    for (auto i = 0u; i < selVector.selectedSize; ++i) {
        auto resultPos = selVector.selectedPositions[i];
        fillEntry(resultPos, srcVector);
    }
}

void CaseExpressionEvaluator::fillAll(ValueVector* srcVector) {
    auto resultSelVector = resultVector->state->selVector.get();
    for (auto i = 0u; i < resultSelVector->selectedSize; ++i) {
        auto resultPos = resultSelVector->selectedPositions[i];
        fillEntry(resultPos, srcVector);
    }
}

void CaseExpressionEvaluator::fillEntry(sel_t resultPos, ValueVector* srcVector) {
    if (filledMask[resultPos]) {
        return;
    }
    filledMask[resultPos] = true;
    auto srcPos =
        srcVector->state->isFlat() ? srcVector->state->selVector->selectedPositions[0] : resultPos;
    resultVector->copyFromVectorData(resultPos, srcVector, srcPos);
}

} // namespace evaluator
} // namespace kuzu
