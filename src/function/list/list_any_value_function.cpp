#include "function/list/functions/list_any_value_function.h"

#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    auto resultType = ListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int32_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int16_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int8_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint64_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint32_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint16_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint8_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int128_t, ListAnyValue>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, double, ListAnyValue>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, float, ListAnyValue>;
    } break;
    case LogicalTypeID::BOOL: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint8_t, ListAnyValue>;
    } break;
    case LogicalTypeID::STRING: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, ku_string_t, ListAnyValue>;
    } break;
    case LogicalTypeID::DATE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, date_t, ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, timestamp_t, ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, timestamp_ms_t, ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, timestamp_ns_t, ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            timestamp_sec_t, ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, timestamp_tz_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INTERVAL: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, interval_t, ListAnyValue>;
    } break;
    case LogicalTypeID::LIST: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, internalID_t, ListAnyValue>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return FunctionBindData::getSimpleBindData(arguments, *resultType);
}

function_set ListAnyValueFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::ANY, nullptr, nullptr, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
