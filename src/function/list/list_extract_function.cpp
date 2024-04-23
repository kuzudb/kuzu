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
    auto resultType = ListType::getChildType(&arguments[0]->dataType);
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    switch (resultType->getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, uint8_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT64: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, int64_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT32: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, int32_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT16: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, int16_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT8: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, int8_t, ListExtract>;
    } break;
    case PhysicalTypeID::UINT64: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, uint64_t, ListExtract>;
    } break;
    case PhysicalTypeID::UINT32: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, uint32_t, ListExtract>;
    } break;
    case PhysicalTypeID::UINT16: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, uint16_t, ListExtract>;
    } break;
    case PhysicalTypeID::UINT8: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, uint8_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT128: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, int128_t, ListExtract>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, double, ListExtract>;
    } break;
    case PhysicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, float, ListExtract>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, interval_t, ListExtract>;
    } break;
    case PhysicalTypeID::STRING: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, ku_string_t, ListExtract>;
    } break;
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, list_entry_t, ListExtract>;
    } break;
    case PhysicalTypeID::STRUCT: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, struct_entry_t, ListExtract>;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, internalID_t, ListExtract>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType());
    paramTypes.push_back(LogicalType(function->parameterTypeIDs[1]));
    return std::make_unique<FunctionBindData>(std::move(paramTypes), resultType->copy());
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
