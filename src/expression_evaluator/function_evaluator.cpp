#include "expression_evaluator/function_evaluator.h"

#include "binder/expression/function_expression.h"
#include "function/struct/vector_struct_operations.h"

using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    execFunc = ((binder::ScalarFunctionExpression&)*expression).execFunc;
    if (expression->dataType.getLogicalTypeID() == LogicalTypeID::BOOL) {
        selectFunc = ((binder::ScalarFunctionExpression&)*expression).selectFunc;
    }
    for (auto& child : children) {
        parameters.push_back(child->resultVector);
    }
}

void FunctionExpressionEvaluator::evaluate() {
    for (auto& child : children) {
        child->evaluate();
    }
    if (execFunc != nullptr) {
        // Some functions are evaluated at compile time (e.g. struct_extract).
        execFunc(parameters, *resultVector);
    }
}

bool FunctionExpressionEvaluator::select(SelectionVector& selVector) {
    for (auto& child : children) {
        child->evaluate();
    }
    // Temporary code path for function whose return type is BOOL but select interface is not
    // implemented (e.g. list_contains). We should remove this if statement eventually.
    if (selectFunc == nullptr) {
        assert(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
        execFunc(parameters, *resultVector);
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
    return selectFunc(parameters, selVector);
}

std::unique_ptr<BaseExpressionEvaluator> FunctionExpressionEvaluator::clone() {
    std::vector<std::unique_ptr<BaseExpressionEvaluator>> clonedChildren;
    for (auto& child : children) {
        clonedChildren.push_back(child->clone());
    }
    return make_unique<FunctionExpressionEvaluator>(expression, std::move(clonedChildren));
}

void FunctionExpressionEvaluator::resolveResultVector(
    const ResultSet& resultSet, MemoryManager* memoryManager) {
    auto& functionExpression = (binder::ScalarFunctionExpression&)*expression;
    auto functionName = functionExpression.getFunctionName();
    if (functionName == STRUCT_EXTRACT_FUNC_NAME || functionName == UNION_EXTRACT_FUNC_NAME) {
        auto& bindData = (function::StructExtractBindData&)*functionExpression.getBindData();
        resultVector =
            StructVector::getFieldVector(children[0]->resultVector.get(), bindData.childIdx);
    } else {
        resultVector = std::make_shared<ValueVector>(expression->dataType, memoryManager);
    }
    std::vector<BaseExpressionEvaluator*> inputEvaluators;
    inputEvaluators.reserve(children.size());
    for (auto& child : children) {
        inputEvaluators.push_back(child.get());
    }
    resolveResultStateFromChildren(inputEvaluators);
    // TODO(Ziyi): We should move result valueVector state resolution to each function.
    if (functionExpression.getFunctionName() == STRUCT_PACK_FUNC_NAME) {
        // Our goal is to make the state of the resultVector consistent with its children vectors.
        // If the resultVector and inputVector are in different dataChunks, we should create a new
        // child valueVector, which shares the state with the resultVector, instead of reusing the
        // inputVector.
        for (auto i = 0u; i < inputEvaluators.size(); i++) {
            auto inputEvaluator = inputEvaluators[i];
            if (inputEvaluator->resultVector->state == resultVector->state) {
                common::StructVector::referenceVector(
                    resultVector.get(), i, inputEvaluator->resultVector);
            }
        }
    } else if (functionExpression.getFunctionName() == UNION_VALUE_FUNC_NAME) {
        assert(inputEvaluators.size() == 1);
        resultVector->setState(inputEvaluators[0]->resultVector->state);
        common::UnionVector::getTagVector(resultVector.get())
            ->setState(inputEvaluators[0]->resultVector->state);
        common::UnionVector::referenceVector(
            resultVector.get(), 0 /* fieldIdx */, inputEvaluators[0]->resultVector);
    }
}

} // namespace evaluator
} // namespace kuzu
