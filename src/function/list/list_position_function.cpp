#include "function/list/functions/list_position_function.h"

#include "common/type_utils.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> ListPositionBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = function->ptrCast<ScalarFunction>();
    TypeUtils::visit(arguments[1]->getDataType().getPhysicalType(), [&scalarFunction]<typename T>(
                                                                        T) {
        scalarFunction->execFunc =
            ScalarFunction::BinaryExecListStructFunction<list_entry_t, T, int64_t, ListPosition>;
    });
    return FunctionBindData::getSimpleBindData(arguments, LogicalType::INT64());
}

function_set ListPositionFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::INT64,
        ListPositionBindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
