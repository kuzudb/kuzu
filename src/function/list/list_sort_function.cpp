#include "function/list/functions/list_sort_function.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "function/list/functions/list_reverse_sort_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename T>
static void getListSortExecFunction(const binder::expression_vector& arguments,
    scalar_func_exec_t& func) {
    if (arguments.size() == 1) {
        func = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, ListSort<T>>;
        return;
    } else if (arguments.size() == 2) {
        func = ScalarFunction::BinaryExecListStructFunction<list_entry_t, ku_string_t, list_entry_t,
            ListSort<T>>;
        return;
    } else if (arguments.size() == 3) {
        func = ScalarFunction::TernaryExecListStructFunction<list_entry_t, ku_string_t, ku_string_t,
            list_entry_t, ListSort<T>>;
        return;
    } else {
        throw RuntimeException("Invalid number of arguments");
    }
}

template<typename T>
static void getListReverseSortExecFunction(const binder::expression_vector& arguments,
    scalar_func_exec_t& func) {
    if (arguments.size() == 1) {
        func = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t,
            ListReverseSort<T>>;
        return;
    } else if (arguments.size() == 2) {
        func = ScalarFunction::BinaryExecListStructFunction<list_entry_t, ku_string_t, list_entry_t,
            ListReverseSort<T>>;
        return;
    } else {
        throw RuntimeException("Invalid number of arguments");
    }
}

static std::unique_ptr<FunctionBindData> ListSortBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    if (arguments[0]->dataType.getLogicalTypeID() == common::LogicalTypeID::ANY) {
        throw BinderException(stringFormat("Cannot resolve recursive data type for expression {}",
            arguments[0]->toString()));
    }
    switch (ListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getListSortExecFunction<int64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getListSortExecFunction<int32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getListSortExecFunction<int16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT8: {
        getListSortExecFunction<int8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT64: {
        getListSortExecFunction<uint64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT32: {
        getListSortExecFunction<uint32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT16: {
        getListSortExecFunction<uint16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT8: {
        getListSortExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT128: {
        getListSortExecFunction<int128_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getListSortExecFunction<double>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getListSortExecFunction<float>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getListSortExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getListSortExecFunction<ku_string_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getListSortExecFunction<date_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        getListSortExecFunction<timestamp_ms_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        getListSortExecFunction<timestamp_ns_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        getListSortExecFunction<timestamp_sec_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        getListSortExecFunction<timestamp_tz_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getListSortExecFunction<timestamp_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getListSortExecFunction<interval_t>(arguments, scalarFunction->execFunc);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return FunctionBindData::getSimpleBindData(arguments, arguments[0]->getDataType());
}

static std::unique_ptr<FunctionBindData> ListReverseSortBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    switch (ListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getListReverseSortExecFunction<int64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getListReverseSortExecFunction<int32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getListReverseSortExecFunction<int16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT8: {
        getListReverseSortExecFunction<int8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT64: {
        getListReverseSortExecFunction<uint64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT32: {
        getListReverseSortExecFunction<uint32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT16: {
        getListReverseSortExecFunction<uint16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT8: {
        getListReverseSortExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT128: {
        getListReverseSortExecFunction<int128_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getListReverseSortExecFunction<double>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getListReverseSortExecFunction<float>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getListReverseSortExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getListReverseSortExecFunction<ku_string_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getListReverseSortExecFunction<date_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        getListReverseSortExecFunction<timestamp_ms_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        getListReverseSortExecFunction<timestamp_ns_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        getListReverseSortExecFunction<timestamp_sec_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        getListReverseSortExecFunction<timestamp_tz_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getListReverseSortExecFunction<timestamp_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getListReverseSortExecFunction<interval_t>(arguments, scalarFunction->execFunc);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType());
    for (auto i = 1u; i < arguments.size(); ++i) {
        paramTypes.push_back(LogicalType(function->parameterTypeIDs[i]));
    }
    return std::make_unique<FunctionBindData>(std::move(paramTypes),
        arguments[0]->getDataType().copy());
}

function_set ListSortFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::LIST, nullptr, nullptr, ListSortBindFunc));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING}, LogicalTypeID::LIST,
        nullptr, nullptr, ListSortBindFunc));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::LIST, nullptr, nullptr, ListSortBindFunc));
    return result;
}

function_set ListReverseSortFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::LIST, nullptr, nullptr, ListReverseSortBindFunc));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING}, LogicalTypeID::LIST,
        nullptr, nullptr, ListReverseSortBindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
