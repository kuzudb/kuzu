#include "common/exception/binder.h"
#include "expression_evaluator/lambda_evaluator.h"
#include "expression_evaluator/list_slice_info.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    if (input.arguments[1]->expressionType != ExpressionType::LAMBDA) {
        throw BinderException(stringFormat(
            "The second argument of LIST_TRANSFORM should be a lambda expression but got {}.",
            ExpressionTypeUtil::toString(input.arguments[1]->expressionType)));
    }
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(input.arguments[0]->getDataType().copy());
    paramTypes.push_back(input.arguments[1]->getDataType().copy());
    return std::make_unique<FunctionBindData>(std::move(paramTypes),
        LogicalType::LIST(input.arguments[1]->getDataType().copy()));
}

static void copyEvaluatedDataToResult(common::ValueVector& result,
    evaluator::ListLambdaBindData* listLambdaBindData) {
    auto& sliceInfo = *listLambdaBindData->sliceInfo;
    auto dstDataVector = ListVector::getDataVector(&result);
    for (sel_t i = 0; i < sliceInfo.getSliceSize(); ++i) {
        const auto [listEntryPos, dataOffset] = sliceInfo.getPos(i);
        const auto srcIdx = listLambdaBindData->lambdaParamEvaluators.empty() ? 0 : i;
        sel_t srcPos =
            listLambdaBindData->rootEvaluator->resultVector->state->getSelVector()[srcIdx];
        dstDataVector->copyFromVectorData(dataOffset,
            listLambdaBindData->rootEvaluator->resultVector.get(), srcPos);
        dstDataVector->setNull(dataOffset,
            listLambdaBindData->rootEvaluator->resultVector->isNull(srcPos));
    }
}

static void copyListEntriesToResult(const common::ValueVector& inputVector,
    const common::SelectionVector& inputSelVector, common::ValueVector& result) {
    for (uint64_t i = 0; i < inputSelVector.getSelSize(); ++i) {
        auto pos = inputSelVector[i];
        result.setNull(pos, inputVector.isNull(pos));

        auto inputList = inputVector.getValue<list_entry_t>(pos);
        ListVector::addList(&result, inputList.size);
        result.setValue(pos, inputList);
    }
}

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& input,
    const std::vector<common::SelectionVector*>& inputSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* bindData) {
    auto listLambdaBindData = reinterpret_cast<evaluator::ListLambdaBindData*>(bindData);
    auto* sliceInfo = listLambdaBindData->sliceInfo;
    auto savedParamStates =
        sliceInfo->overrideAndSaveParamStates(listLambdaBindData->lambdaParamEvaluators);

    listLambdaBindData->rootEvaluator->evaluate();
    copyEvaluatedDataToResult(result, listLambdaBindData);

    auto& inputVector = *input[0];
    const auto& inputSelVector = *inputSelVectors[0];
    KU_ASSERT(input.size() == 2);
    if (!listLambdaBindData->lambdaParamEvaluators.empty()) {
        if (sliceInfo->done()) {
            ListVector::copyListEntryAndBufferMetaData(result, *resultSelVector, inputVector,
                inputSelVector);
        }
    } else {
        if (sliceInfo->done()) {
            copyListEntriesToResult(inputVector, inputSelVector, result);
        }
    }

    sliceInfo->restoreParamStates(listLambdaBindData->lambdaParamEvaluators,
        std::move(savedParamStates));
}

function_set ListTransformFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        execFunc);
    function->bindFunc = bindFunc;
    function->isListLambda = true;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
