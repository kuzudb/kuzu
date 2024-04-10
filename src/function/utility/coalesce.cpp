#include "common/exception/binder.h"
#include "function/scalar_function.h"
#include "function/utility/scalar_utility_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    if (arguments.empty()) {
        throw BinderException("COALESCE requires at least one argument");
    }
    // TODO (Manh): Use maxLogicalTypeID to obtain resultType and enable implicit casting for
    // parameters
    auto resultType = LogicalType(LogicalTypeID::ANY);
    for (auto& argument : arguments) {
        auto& parameterType = argument->getDataTypeReference();
        if (parameterType.getLogicalTypeID() != LogicalTypeID::ANY) {
            resultType = parameterType;
            break;
        }
    }
    if (resultType.getLogicalTypeID() == LogicalTypeID::ANY) {
        resultType = LogicalType(LogicalTypeID::STRING);
    }
    for (auto& argument : arguments) {
        auto& parameterType = argument->getDataTypeReference();
        if (parameterType != resultType) {
            if (parameterType.getLogicalTypeID() == LogicalTypeID::ANY) {
                argument->cast(resultType);
            } else {
                throw BinderException(
                    stringFormat("Cannot mix values of type {} and {} in COALESCE function - an "
                                 "explicit cast is required",
                        parameterType.toString(), resultType.toString()));
            }
        }
    }
    return std::make_unique<FunctionBindData>(resultType.copy());
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
    void* /*dataPtr*/) {
    for (auto i = 0u; i < result.state->selVector->selectedSize; ++i) {
        auto resultPos = result.state->selVector->selectedPositions[i];
        auto isNull = true;
        for (auto j = 0u; j < params.size(); ++j) {
            const auto& parameter = params[j];
            auto paramPos = parameter->state->isFlat() ?
                                parameter->state->selVector->selectedPositions[0] :
                                resultPos;
            if (!parameter->isNull(paramPos)) {
                result.copyFromVectorData(resultPos, parameter.get(), paramPos);
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
    auto allFlat = true;
    auto unFlatVectorPos = 0u;
    for (auto i = 0u; i < params.size(); ++i) {
        KU_ASSERT(params[i]->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
        if (!params[i]->state->isFlat()) {
            unFlatVectorPos = i;
            allFlat = false;
        }
    }
    if (allFlat) {
        auto resultValue = false;
        for (auto i = 0u; i < params.size(); ++i) {
            auto pos = params[i]->state->selVector->selectedPositions[0];
            if (!params[i]->isNull(pos)) {
                resultValue = params[i]->getValue<bool>(pos);
                break;
            }
        }
        return resultValue == true;
    } else {
        auto numSelectedValues = 0u;
        auto selectedPositionsBuffer = selVector.getMultableBuffer();
        for (auto i = 0u; i < params[unFlatVectorPos]->state->selVector->selectedSize; ++i) {
            auto resultPos = params[unFlatVectorPos]->state->selVector->selectedPositions[i];
            auto resultValue = false;
            for (auto j = 0u; j < params.size(); ++j) {
                const auto& parameter = params[j];
                auto paramPos = parameter->state->isFlat() ?
                                    parameter->state->selVector->selectedPositions[0] :
                                    resultPos;
                if (!parameter->isNull(paramPos)) {
                    resultValue = parameter->getValue<bool>(paramPos);
                    break;
                }
            }
            selectedPositionsBuffer[numSelectedValues] = resultPos;
            numSelectedValues += (resultValue == true);
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }
}

function_set CoalesceFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::ANY, execFunc, selectFunc, bindFunc, true /*  isVarLength */));
    return functionSet;
}

} // namespace function
} // namespace kuzu
