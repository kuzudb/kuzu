#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "function/scalar_function.h"
#include "function/utility/vector_utility_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    if (arguments.empty()) {
        throw BinderException("COALESCE requires at least one argument");
    }
    LogicalType resultType(LogicalTypeID::ANY);
    binder::ExpressionUtil::tryCombineDataType(arguments, resultType);
    if (resultType.getLogicalTypeID() == LogicalTypeID::ANY) {
        resultType = *LogicalType::STRING();
    }
    auto bindData = std::make_unique<FunctionBindData>(resultType.copy());
    for (auto& _ : arguments) {
        (void)_;
        bindData->paramTypes.push_back(resultType);
    }
    return bindData;
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
    void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto i = 0u; i < result.state->selVector->selectedSize; ++i) {
        auto resultPos = result.state->selVector->selectedPositions[i];
        auto isNull = true;
        for (auto& param : params) {
            auto paramPos =
                param->state->isFlat() ? param->state->selVector->selectedPositions[0] : resultPos;
            if (!param->isNull(paramPos)) {
                result.copyFromVectorData(resultPos, param.get(), paramPos);
                isNull = false;
                break;
            }
        }
        result.setNull(resultPos, isNull);
    }
}

static bool selectFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
    SelectionVector& selVector) {
    KU_ASSERT(!params.empty());
    auto unFlatVectorIdx = 0u;
    for (auto i = 0u; i < params.size(); ++i) {
        if (!params[i]->state->isFlat()) {
            unFlatVectorIdx = i;
            break;
        }
    }
    auto numSelectedValues = 0u;
    auto selectedPositionsBuffer = selVector.getMultableBuffer();
    for (auto i = 0u; i < params[unFlatVectorIdx]->state->selVector->selectedSize; ++i) {
        auto resultPos = params[unFlatVectorIdx]->state->selVector->selectedPositions[i];
        auto resultValue = false;
        for (auto& param : params) {
            auto paramPos =
                param->state->isFlat() ? param->state->selVector->selectedPositions[0] : resultPos;
            if (!param->isNull(paramPos)) {
                resultValue = param->getValue<bool>(paramPos);
                break;
            }
        }
        selectedPositionsBuffer[numSelectedValues] = resultPos;
        numSelectedValues += resultValue;
    }
    selVector.selectedSize = numSelectedValues;
    return numSelectedValues > 0;
}

function_set CoalesceFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::ANY, execFunc, selectFunc, bindFunc);
    function->isVarLength = true;
    functionSet.push_back(std::move(function));
    return functionSet;
}

function_set IfNullFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY, LogicalTypeID::ANY}, LogicalTypeID::ANY,
        execFunc, selectFunc, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
