#include "common/exception/binder.h"
#include "expression_evaluator/lambda_evaluator.h"
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

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& input,
    const std::vector<common::SelectionVector*>& inputSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* bindData) {
    auto listLambdaBindData = reinterpret_cast<evaluator::ListLambdaBindData*>(bindData);
    auto& inputVector = *input[0];
    const auto& inputSelVector = *inputSelVectors[0];

    auto listSize = ListVector::getDataVectorSize(&inputVector);
    for (auto& lambdaParamEvaluator : listLambdaBindData->lambdaParamEvaluators) {
        auto param = lambdaParamEvaluator->resultVector.get();
        param->state->getSelVectorUnsafe().setSelSize(listSize);
    }
    listLambdaBindData->rootEvaluator->evaluate();
    KU_ASSERT(input.size() == 2);
    if (!listLambdaBindData->lambdaParamEvaluators.empty()) {
        ListVector::copyListEntryAndBufferMetaData(result, *resultSelVector, inputVector,
            inputSelVector);
    } else {
        auto srcPos = inputSelVector[0];
        auto dstDataVector = ListVector::getDataVector(&result);
        for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
            auto inputList = inputVector.getValue<list_entry_t>(inputSelVector[0]);
            auto pos = (*resultSelVector)[i];
            if (inputVector.isNull(srcPos)) {
                result.setNull(pos, true);
            } else {
                auto dstLst = ListVector::addList(&inputVector, inputList.size);
                for (auto j = 0u; j < dstLst.size; j++) {
                    dstDataVector->copyFromVectorData(dstLst.offset + j,
                        listLambdaBindData->rootEvaluator->resultVector.get(),
                        listLambdaBindData->rootEvaluator->resultVector->state->getSelVector()[0]);
                }
                result.setValue(pos, dstLst);
            }
        }
    }
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
