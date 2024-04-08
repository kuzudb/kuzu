#include "common/exception/binder.h"
#include "function/scalar_function.h"
#include "function/utility/scalar_utility_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static LogicalType getValidLogicalType(const binder::expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() != LogicalTypeID::ANY) {
            return expression->dataType;
        }
    }
    return LogicalType(LogicalTypeID::ANY);
}

static LogicalType getResultType(const binder::expression_vector& arguments) {
    auto resultType = getValidLogicalType(arguments);
    if (resultType.getLogicalTypeID() == LogicalTypeID::ANY) {
        resultType = LogicalType(LogicalTypeID::STRING);
    }
    for (auto& argument : arguments) {
        auto& parameterType = argument->getDataTypeReference();
        if (parameterType != resultType) {
            if (parameterType.getLogicalTypeID() == LogicalTypeID::ANY) {
                argument->cast(LogicalType(LogicalTypeID::STRING));
            } else {
                throw BinderException(
                    stringFormat("Cannot mix values of type {} and {} in COALESCE function - an "
                                 "explicit cast is required",
                        parameterType.toString(), resultType.toString()));
            }
        }
    }
    return resultType;
}

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    if (arguments.empty()) {
        throw BinderException("COALESCE() requires at least one argument");
    }
    auto resultType = getResultType(arguments).copy();
    return std::make_unique<FunctionBindData>(std::move(resultType));
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    for (auto selectedPos = 0u; selectedPos < result.state->selVector->selectedSize;
         ++selectedPos) {
        auto resultPos = result.state->selVector->selectedPositions[selectedPos];
        bool isNull = true;
        for (auto i = 0u; i < parameters.size(); ++i) {
            const auto& parameter = parameters[i];
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

function_set CoalesceFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::ANY, execFunc, nullptr, bindFunc, true /*  isVarLength */));
    return functionSet;
}

} // namespace function
} // namespace kuzu
