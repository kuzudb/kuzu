#include "function/scalar_function.h"
#include "function/utility/vector_utility_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    logical_type_vec_t paramTypes;
    for (auto& argument : arguments) {
        if (argument->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            paramTypes.push_back(LogicalType::STRING());
        } else {
            paramTypes.push_back(argument->getDataType().copy());
        }
    }
    auto bindData = std::make_unique<FunctionBindData>(paramTypes[0].copy());
    bindData->paramTypes = std::move(paramTypes);
    return bindData;
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
    void* /*dataPtr*/) {
    KU_ASSERT(params.size() == 2);
    result.resetAuxiliaryBuffer();
    for (auto i = 0u; i < result.state->getSelVector().getSelSize(); ++i) {
        auto resultPos = result.state->getSelVector()[i];
        auto firstParamPos =
            params[0]->state->isFlat() ? params[0]->state->getSelVector()[0] : resultPos;
        auto secondParamPos =
            params[1]->state->isFlat() ? params[1]->state->getSelVector()[0] : resultPos;
        if (params[1]->isNull(secondParamPos) || params[0]->isNull(firstParamPos)) {
            result.setNull(resultPos, true);
        } else {
            result.setNull(resultPos, false);
            result.copyFromVectorData(resultPos, params[0].get(), firstParamPos);
        }
    }
}

static bool selectFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
    SelectionVector& selVector) {
    KU_ASSERT(params.size() == 2);
    auto unFlatVectorIdx = 0u;
    for (auto i = 0u; i < params.size(); ++i) {
        if (!params[i]->state->isFlat()) {
            unFlatVectorIdx = i;
            break;
        }
    }
    auto numSelectedValues = 0u;
    auto selectedPositionsBuffer = selVector.getMultableBuffer();
    for (auto i = 0u; i < params[unFlatVectorIdx]->state->getSelVector().getSelSize(); ++i) {
        auto resultPos = params[unFlatVectorIdx]->state->getSelVector()[i];
        auto resultValue = false;
        auto firstParamPos =
            params[0]->state->isFlat() ? params[0]->state->getSelVector()[0] : resultPos;
        auto secondParamPos =
            params[1]->state->isFlat() ? params[1]->state->getSelVector()[0] : resultPos;
        if (!params[1]->isNull(secondParamPos) && !params[0]->isNull(firstParamPos)) {
            resultValue = params[0]->getValue<bool>(firstParamPos);
        }
        selectedPositionsBuffer[numSelectedValues] = resultPos;
        numSelectedValues += resultValue;
    }
    selVector.setSelSize(numSelectedValues);
    return numSelectedValues > 0;
}

function_set ConstantOrNullFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY, LogicalTypeID::ANY}, LogicalTypeID::ANY,
        execFunc, selectFunc, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
