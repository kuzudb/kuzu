#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct Range {
    // range function:
    // - include end
    // - when start = end: there is only one element in result list
    // - when end - start are of opposite sign of step, the result will be empty
    // - default step = 1
    template<typename T>
    static void operation(T& end, list_entry_t& result, ValueVector& endVector,
        ValueVector& resultVector) {
        T step = 1;
        T start = 0;
        operation(start, end, step, result, endVector, resultVector);
    }

    template<typename T>
    static void operation(T& start, T& end, list_entry_t& result, ValueVector& leftVector,
        ValueVector& /*rightVector*/, ValueVector& resultVector) {
        T step = 1;
        operation(start, end, step, result, leftVector, resultVector);
    }

    template<typename T>
    static void operation(T& start, T& end, T& step, list_entry_t& result,
        ValueVector& /*inputVector*/, ValueVector& resultVector) {
        if (step == 0) {
            throw RuntimeException("Step of range cannot be 0.");
        }

        // start, start + step, start + 2step, ..., end
        T number = start;
        auto size = ((end - start) * 1.0 / step);
        size < 0 ? size = 0 : size = (int64_t)(size + 1);

        result = ListVector::addList(&resultVector, (int64_t)size);
        auto resultDataVector = ListVector::getDataVector(&resultVector);
        for (auto i = 0u; i < (int64_t)size; i++) {
            resultDataVector->setValue(result.offset + i, number);
            number += step;
        }
    }
};

static scalar_func_exec_t getBinaryExecFunc(const LogicalType& type) {
    scalar_func_exec_t execFunc;
    TypeUtils::visit(
        type.getLogicalTypeID(),
        [&execFunc]<IntegerTypes T>(T) {
            execFunc = ScalarFunction::BinaryExecListStructFunction<T, T, list_entry_t, Range>;
        },
        [](auto) { KU_UNREACHABLE; });
    return execFunc;
}

static scalar_func_exec_t getTernaryExecFunc(const LogicalType& type) {
    scalar_func_exec_t execFunc;
    TypeUtils::visit(
        type.getLogicalTypeID(),
        [&execFunc]<IntegerTypes T>(T) {
            execFunc = ScalarFunction::TernaryExecListStructFunction<T, T, T, list_entry_t, Range>;
        },
        [](auto) { KU_UNREACHABLE; });
    return execFunc;
}

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    auto type = LogicalType(function->parameterTypeIDs[0]);
    auto resultType = LogicalType::LIST(type.copy());
    auto bindData = std::make_unique<FunctionBindData>(std::move(resultType));
    for (auto& _ : arguments) {
        (void)_;
        bindData->paramTypes.push_back(type);
    }
    return bindData;
}

function_set ListRangeFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getIntegerTypeIDs()) {
        // start, end
        result.push_back(
            std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{typeID, typeID},
                LogicalTypeID::LIST, getBinaryExecFunc(LogicalType{typeID}), bindFunc));
        // start, end, step
        result.push_back(std::make_unique<ScalarFunction>(name,
            std::vector<LogicalTypeID>{typeID, typeID, typeID}, LogicalTypeID::LIST,
            getTernaryExecFunc(LogicalType{typeID}), bindFunc));
    }
    return result;
}

} // namespace function
} // namespace kuzu
