#include "common/exception/binder.h"
#include "expression_evaluator/lambda_evaluator.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
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
    common::ValueVector& result, void* bindData) {
    auto listLambdaBindData = reinterpret_cast<evaluator::ListLambdaBindData*>(bindData);
    auto inputVector = input[0].get();
    auto listSize = ListVector::getDataVectorSize(inputVector);
    auto lambdaParamVector = listLambdaBindData->lambdaParamEvaluators[0]->resultVector.get();
    lambdaParamVector->state->getSelVectorUnsafe().setSelSize(listSize);
    listLambdaBindData->rootEvaluator->evaluate();
    KU_ASSERT(input.size() == 2);
    ListVector::setDataVector(&result, input[1]);
    ListVector::copyListEntryAndBufferMetaData(result, *inputVector);
}

function_set ListTransformFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        execFunc, bindFunc);
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
