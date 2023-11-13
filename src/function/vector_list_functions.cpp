#include "function/list/vector_list_functions.h"

#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "function/list/functions/list_any_value_function.h"
#include "function/list/functions/list_append_function.h"
#include "function/list/functions/list_concat_function.h"
#include "function/list/functions/list_contains_function.h"
#include "function/list/functions/list_distinct_function.h"
#include "function/list/functions/list_extract_function.h"
#include "function/list/functions/list_len_function.h"
#include "function/list/functions/list_position_function.h"
#include "function/list/functions/list_prepend_function.h"
#include "function/list/functions/list_product_function.h"
#include "function/list/functions/list_range_function.h"
#include "function/list/functions/list_reverse_sort_function.h"
#include "function/list/functions/list_slice_function.h"
#include "function/list/functions/list_sort_function.h"
#include "function/list/functions/list_sum_function.h"
#include "function/list/functions/list_unique_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::string getListFunctionIncompatibleChildrenTypeErrorMsg(
    const std::string& functionName, const LogicalType& left, const LogicalType& right) {
    return std::string("Cannot bind " + functionName + " with parameter type " + left.toString() +
                       " and " + right.toString() + ".");
}

void ListCreationFunction::execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    KU_ASSERT(result.dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST);
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < result.state->selVector->selectedSize;
         ++selectedPos) {
        auto pos = result.state->selVector->selectedPositions[selectedPos];
        auto resultEntry = ListVector::addList(&result, parameters.size());
        result.setValue(pos, resultEntry);
        auto resultDataVector = ListVector::getDataVector(&result);
        auto resultPos = resultEntry.offset;
        for (auto i = 0u; i < parameters.size(); i++) {
            auto parameter = parameters[i];
            auto paramPos = parameter->state->isFlat() ?
                                parameter->state->selVector->selectedPositions[0] :
                                pos;
            resultDataVector->copyFromVectorData(resultPos++, parameter.get(), paramPos);
        }
    }
}

static LogicalType getValidLogicalType(const binder::expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() != LogicalTypeID::ANY) {
            return expression->dataType;
        }
    }
    return LogicalType(common::LogicalTypeID::ANY);
}

std::unique_ptr<FunctionBindData> ListCreationFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    // ListCreation requires all parameters to have the same type or be ANY type. The result type of
    // listCreation can be determined by the first non-ANY type parameter. If all parameters have
    // dataType ANY, then the resultType will be INT64[] (default type).
    auto childType = getValidLogicalType(arguments);
    if (childType.getLogicalTypeID() == common::LogicalTypeID::ANY) {
        childType = LogicalType(common::LogicalTypeID::STRING);
    }
    // Cast parameters with ANY dataType to resultChildType.
    for (auto& argument : arguments) {
        auto& parameterType = argument->getDataTypeReference();
        if (parameterType != childType) {
            if (parameterType.getLogicalTypeID() == LogicalTypeID::ANY) {
                binder::ExpressionBinder::resolveAnyDataType(*argument, childType);
            } else {
                throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
                    LIST_CREATION_FUNC_NAME, arguments[0]->getDataType(), argument->getDataType()));
            }
        }
    }
    auto varListTypeInfo = std::make_unique<VarListTypeInfo>(childType.copy());
    auto resultType = LogicalType{LogicalTypeID::VAR_LIST, std::move(varListTypeInfo)};
    return std::make_unique<FunctionBindData>(resultType);
}

function_set ListCreationFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_CREATION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST, execFunc, nullptr,
        bindFunc, true /*  isVarLength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListRangeFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    KU_ASSERT(arguments[0]->dataType == arguments[1]->dataType);
    auto varListTypeInfo = std::make_unique<VarListTypeInfo>(
        std::make_unique<LogicalType>(arguments[0]->dataType.getLogicalTypeID()));
    auto resultType = LogicalType{LogicalTypeID::VAR_LIST, std::move(varListTypeInfo)};
    return std::make_unique<FunctionBindData>(resultType);
}

function_set ListRangeFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getIntegerLogicalTypeIDs()) {
        // start, end
        result.push_back(std::make_unique<ScalarFunction>(LIST_RANGE_FUNC_NAME,
            std::vector<LogicalTypeID>{typeID, typeID}, LogicalTypeID::VAR_LIST,
            ListFunction::getBinaryListExecFuncSwitchAll<Range, list_entry_t>(LogicalType{typeID}),
            nullptr, bindFunc, false));
        // start, end, step
        result.push_back(std::make_unique<ScalarFunction>(LIST_RANGE_FUNC_NAME,
            std::vector<LogicalTypeID>{typeID, typeID, typeID}, LogicalTypeID::VAR_LIST,
            ListFunction::getTernaryListExecFuncSwitchAll<Range, list_entry_t>(LogicalType{typeID}),
            nullptr, bindFunc, false));
    }
    return result;
}

function_set SizeFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(SIZE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<list_entry_t, int64_t, ListLen>, true /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(CARDINALITY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<list_entry_t, int64_t, ListLen>, true /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(SIZE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<ku_string_t, int64_t, ListLen>, true /* isVarlength*/));
    return result;
}

template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
static void BinaryExecListExtractFunction(
    const std::vector<std::shared_ptr<common::ValueVector>>& params, common::ValueVector& result,
    void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(params.size() == 2);
    BinaryFunctionExecutor::executeListExtract<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
        *params[0], *params[1], result);
}

std::unique_ptr<FunctionBindData> ListExtractFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
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
            BinaryExecListExtractFunction<list_entry_t, int64_t, double_t, ListExtract>;
    } break;
    case PhysicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, float_t, ListExtract>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, interval_t, ListExtract>;
    } break;
    case PhysicalTypeID::STRING: {
        scalarFunction->execFunc =
            BinaryExecListExtractFunction<list_entry_t, int64_t, ku_string_t, ListExtract>;
    } break;
    case PhysicalTypeID::VAR_LIST: {
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
    return std::make_unique<FunctionBindData>(*resultType);
}

function_set ListExtractFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::INT64},
        LogicalTypeID::ANY, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(LIST_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryExecFunction<ku_string_t, int64_t, ku_string_t, ListExtract>,
        false /* isVarlength */));
    return result;
}

function_set ListConcatFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, ListConcat>;
    result.push_back(std::make_unique<ScalarFunction>(LIST_CONCAT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::VAR_LIST},
        LogicalTypeID::VAR_LIST, execFunc, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListConcatFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_CONCAT_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

std::unique_ptr<FunctionBindData> ListAppendFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    if (*VarListType::getChildType(&arguments[0]->dataType) != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[0]->getDataType();
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    scalarFunction->execFunc =
        ListFunction::getBinaryListExecFuncSwitchRight<ListAppend, list_entry_t>(
            arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(resultType);
}

function_set ListAppendFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_APPEND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListPrependFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    if (arguments[0]->getDataType().getLogicalTypeID() != LogicalTypeID::ANY &&
        arguments[1]->dataType != *VarListType::getChildType(&arguments[0]->dataType)) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_PREPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[0]->getDataType();
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    scalarFunction->execFunc =
        ListFunction::getBinaryListExecFuncSwitchRight<ListPrepend, list_entry_t>(
            arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(resultType);
}

function_set ListPrependFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_PREPEND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

function_set ListPositionFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_POSITION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::INT64, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListPositionFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    scalarFunction->execFunc =
        ListFunction::getBinaryListExecFuncSwitchRight<ListPosition, int64_t>(
            arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType{LogicalTypeID::INT64});
}

function_set ListContainsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_CONTAINS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::BOOL, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListContainsFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    scalarFunction->execFunc =
        ListFunction::getBinaryListExecFuncSwitchRight<ListContains, uint8_t>(
            arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType{LogicalTypeID::BOOL});
}

function_set ListSliceFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_SLICE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::VAR_LIST, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::VAR_LIST,
        ScalarFunction::TernaryExecListStructFunction<list_entry_t, int64_t, int64_t, list_entry_t,
            ListSlice>,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(LIST_SLICE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryExecListStructFunction<ku_string_t, int64_t, int64_t, ku_string_t,
            ListSlice>,
        false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListSliceFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

function_set ListSortFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::VAR_LIST, LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListSortFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getExecFunction<int64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getExecFunction<int32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getExecFunction<int16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT8: {
        getExecFunction<int8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT64: {
        getExecFunction<uint64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT32: {
        getExecFunction<uint32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT16: {
        getExecFunction<uint16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT8: {
        getExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT128: {
        getExecFunction<int128_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getExecFunction<double_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getExecFunction<float_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getExecFunction<ku_string_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getExecFunction<date_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getExecFunction<timestamp_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getExecFunction<interval_t>(arguments, scalarFunction->execFunc);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

template<typename T>
void ListSortFunction::getExecFunction(
    const binder::expression_vector& arguments, scalar_exec_func& func) {
    if (arguments.size() == 1) {
        func = ScalarFunction::UnaryExecListStructFunction<list_entry_t, list_entry_t, ListSort<T>>;
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

function_set ListReverseSortFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_REVERSE_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(LIST_REVERSE_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListReverseSortFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getExecFunction<int64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getExecFunction<int32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getExecFunction<int16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT8: {
        getExecFunction<int8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT64: {
        getExecFunction<uint64_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT32: {
        getExecFunction<uint32_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT16: {
        getExecFunction<uint16_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::UINT8: {
        getExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INT128: {
        getExecFunction<int128_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getExecFunction<double_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getExecFunction<float_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getExecFunction<uint8_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getExecFunction<ku_string_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getExecFunction<date_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getExecFunction<timestamp_t>(arguments, scalarFunction->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getExecFunction<interval_t>(arguments, scalarFunction->execFunc);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

template<typename T>
void ListReverseSortFunction::getExecFunction(
    const binder::expression_vector& arguments, scalar_exec_func& func) {
    if (arguments.size() == 1) {
        func = ScalarFunction::UnaryExecListStructFunction<list_entry_t, list_entry_t,
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

function_set ListSumFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_SUM_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        ListFunction::bindFuncListAggr<ListSum>, false /* isVarlength*/));
    return result;
}

function_set ListProductFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_PRODUCT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        ListFunction::bindFuncListAggr<ListProduct>, false /* isVarlength*/));
    return result;
}

function_set ListDistinctFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_DISTINCT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListDistinctFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<int16_t>>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<int8_t>>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<uint64_t>>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<uint32_t>>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<uint16_t>>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<uint8_t>>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<int128_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<double_t>>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<float_t>>;
    } break;
    case LogicalTypeID::BOOL: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<timestamp_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            list_entry_t, ListDistinct<internalID_t>>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

function_set ListUniqueFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_UNIQUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListUniqueFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<int16_t>>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<int8_t>>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<uint64_t>>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<uint32_t>>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<uint16_t>>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<uint8_t>>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<int128_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<double_t>>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<float_t>>;
    } break;
    case LogicalTypeID::BOOL: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<timestamp_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecListStructFunction<list_entry_t,
            int64_t, ListUnique<internalID_t>>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(LogicalType(LogicalTypeID::INT64));
}

function_set ListAnyValueFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(LIST_ANY_VALUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::ANY, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListAnyValueFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int64_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int32_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int16_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int8_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, uint64_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, uint32_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, uint16_t, ListAnyValue>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, uint8_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, int128_t, ListAnyValue>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, double_t, ListAnyValue>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, float_t, ListAnyValue>;
    } break;
    case LogicalTypeID::BOOL: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, uint8_t, ListAnyValue>;
    } break;
    case LogicalTypeID::STRING: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, ku_string_t, ListAnyValue>;
    } break;
    case LogicalTypeID::DATE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, date_t, ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, timestamp_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INTERVAL: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, interval_t, ListAnyValue>;
    } break;
    case LogicalTypeID::VAR_LIST: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, list_entry_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecListStructFunction<list_entry_t, internalID_t, ListAnyValue>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(*resultType);
}

} // namespace function
} // namespace kuzu
