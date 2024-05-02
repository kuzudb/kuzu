#include "function/list/functions/list_extract_function.h"

#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
static void BinaryExecListExtractFunction(const std::vector<std::shared_ptr<ValueVector>>& params,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(params.size() == 2);
    BinaryFunctionExecutor::executeListExtract<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(*params[0],
        *params[1], result);
}

static std::unique_ptr<FunctionBindData> ListExtractBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto resultType = ListType::getChildType(arguments[0]->dataType);
    auto scalarFunction = function->ptrCast<ScalarFunction>();
    TypeUtils::visit(resultType.getPhysicalType(), [&scalarFunction]<typename T>(T) {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, T, ListExtract>;
    });
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType());
    paramTypes.push_back(LogicalType(function->parameterTypeIDs[1]));
    return std::make_unique<FunctionBindData>(std::move(paramTypes), resultType.copy());
}

function_set ListExtractFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::INT64}, LogicalTypeID::ANY,
        nullptr, nullptr, ListExtractBindFunc));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryExecFunction<ku_string_t, int64_t, ku_string_t, ListExtract>));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ARRAY, LogicalTypeID::INT64}, LogicalTypeID::ANY,
        nullptr, nullptr, ListExtractBindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
