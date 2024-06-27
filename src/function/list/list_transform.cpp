#include "function/lambda/lambda_function_bind_data.h"
#include "function/list/functions/list_to_string_function.h"
#include "function/list/vector_list_functions.h"

namespace kuzu {
namespace function {

using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType().copy());
    return std::make_unique<LambdaFunctionBindData>(std::move(paramTypes),
        common::LogicalType::LIST(arguments[1]->getDataType().copy()), arguments[1]);
}

static void execute(const std::vector<std::shared_ptr<common::ValueVector>>& operand,
    common::ValueVector& result, void* dataPtr) {
    auto lambdaFuncBindData = reinterpret_cast<LambdaFunctionBindData*>(dataPtr);
    KU_ASSERT(operand.size() == 1);
    auto dataVec = common::ListVector::getSharedDataVector(operand[0].get());
    dataVec->state->getSelVectorUnsafe().setSelSize(
        common::ListVector::getDataVectorSize(operand[0].get()));
    lambdaFuncBindData->evaluator->evaluate();
    memcpy(result.getData(), operand[0]->getData(),
        (operand[0]->state->getSelVector()[operand[0]->state->getSelVector().getSelSize() - 1] +
            1) *
            operand[0]->getNumBytesPerValue());
    result.state = operand[0]->state;
    common::ListVector::setDataVector(&result, lambdaFuncBindData->evaluator->resultVector);
}

function_set ListTransformFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        execute);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
