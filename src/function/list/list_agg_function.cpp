#include "common/exception/binder.h"
#include "common/type_utils.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename OPERATION>
static std::unique_ptr<FunctionBindData> bindFuncListAggr(ScalarBindFuncInput input) {
    auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
    const auto& resultType = ListType::getChildType(input.arguments[0]->dataType);
    TypeUtils::visit(
        resultType,
        [&scalarFunction]<NumericTypes T>(T) {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, T, OPERATION>;
        },
        [&input, &resultType](auto) {
            throw BinderException(stringFormat("Unsupported inner data type for {}: {}",
                input.definition->name, LogicalTypeUtils::toString(resultType.getLogicalTypeID())));
        });
    return FunctionBindData::getSimpleBindData(input.arguments, resultType);
}

struct ListSum {
    template<typename T>
    static void operation(common::list_entry_t& input, T& result, common::ValueVector& inputVector,
        common::ValueVector& /*resultVector*/) {
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);
        result = 0;
        for (auto i = 0u; i < input.size; i++) {
            if (inputDataVector->isNull(input.offset + i)) {
                continue;
            }
            result += inputDataVector->getValue<T>(input.offset + i);
        }
    }
};

function_set ListSumFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::INT64, bindFuncListAggr<ListSum>));
    return result;
}

struct ListProduct {
    template<typename T>
    static void operation(common::list_entry_t& input, T& result, common::ValueVector& inputVector,
        common::ValueVector& /*resultVector*/) {
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);
        result = 1;
        for (auto i = 0u; i < input.size; i++) {
            if (inputDataVector->isNull(input.offset + i)) {
                continue;
            }
            result *= inputDataVector->getValue<T>(input.offset + i);
        }
    }
};

function_set ListProductFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::INT64, bindFuncListAggr<ListProduct>));
    return result;
}

} // namespace function
} // namespace kuzu
