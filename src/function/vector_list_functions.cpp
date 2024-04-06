#include "function/list/vector_list_functions.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/types/int128_t.h"
#include "common/types/interval_t.h"
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
#include "function/list/functions/list_reverse_function.h"
#include "function/list/functions/list_reverse_sort_function.h"
#include "function/list/functions/list_slice_function.h"
#include "function/list/functions/list_sort_function.h"
#include "function/list/functions/list_sum_function.h"
#include "function/list/functions/list_unique_function.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename OPERATION, typename RESULT_TYPE>
static scalar_func_exec_t getBinaryListExecFuncSwitchRight(const LogicalType& rightType) {
    scalar_func_exec_t execFunc;
    switch (rightType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, uint8_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::INT64: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, int64_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::INT32: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, int32_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::INT16: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, int16_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::INT8: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, int8_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::UINT64: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, uint64_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::UINT32: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, uint32_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::UINT16: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, uint16_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::UINT8: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, uint8_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::INT128: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, int128_t, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, double, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::FLOAT: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, float, RESULT_TYPE,
            OPERATION>;
    } break;
    case PhysicalTypeID::STRING: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, ku_string_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, interval_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, internalID_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::LIST: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::STRUCT: {
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, struct_entry_t,
            RESULT_TYPE, OPERATION>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return execFunc;
}

template<typename OPERATION, typename RESULT_TYPE>
static scalar_func_exec_t getBinaryListExecFuncSwitchAll(const LogicalType& type) {
    scalar_func_exec_t execFunc;
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int64_t, int64_t, RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT32: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int32_t, int32_t, RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT16: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int16_t, int16_t, RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT8: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int8_t, int8_t, RESULT_TYPE, OPERATION>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return execFunc;
}

template<typename OPERATION, typename RESULT_TYPE>
static scalar_func_exec_t getTernaryListExecFuncSwitchAll(const LogicalType& type) {
    scalar_func_exec_t execFunc;
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int64_t, int64_t, int64_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT32: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int32_t, int32_t, int32_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT16: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int16_t, int16_t, int16_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT8: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int8_t, int8_t, int8_t,
            RESULT_TYPE, OPERATION>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return execFunc;
}

template<typename OPERATION>
static std::unique_ptr<FunctionBindData> bindFuncListAggr(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    auto resultType = ListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, OPERATION>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int32_t, OPERATION>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int16_t, OPERATION>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int8_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint64_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint32_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint16_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint8_t, OPERATION>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int128_t, OPERATION>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, double, OPERATION>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, float, OPERATION>;
    } break;
    default: {
        throw BinderException(stringFormat("Unsupported inner data type for {}: {}", function->name,
            LogicalTypeUtils::toString(resultType->getLogicalTypeID())));
    }
    }
    return std::make_unique<FunctionBindData>(resultType->copy());
}

static std::string getListFunctionIncompatibleChildrenTypeErrorMsg(const std::string& functionName,
    const LogicalType& left, const LogicalType& right) {
    return std::string("Cannot bind " + functionName + " with parameter type " + left.toString() +
                       " and " + right.toString() + ".");
}

void ListCreationFunction::execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < result.state->selVector->selectedSize;
         ++selectedPos) {
        auto pos = result.state->selVector->selectedPositions[selectedPos];
        auto resultEntry = ListVector::addList(&result, parameters.size());
        result.setValue(pos, resultEntry);
        auto resultDataVector = ListVector::getDataVector(&result);
        auto resultPos = resultEntry.offset;
        for (auto i = 0u; i < parameters.size(); i++) {
            const auto& parameter = parameters[i];
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
    return LogicalType(LogicalTypeID::ANY);
}

static std::unique_ptr<FunctionBindData> ListCreationBindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    auto resultType = LogicalType::LIST(ListCreationFunction::getChildType(arguments).copy());
    return std::make_unique<FunctionBindData>(std::move(resultType));
}

LogicalType ListCreationFunction::getChildType(const binder::expression_vector& arguments) {
    // ListCreation requires all parameters to have the same type or be ANY type. The result type of
    // listCreation can be determined by the first non-ANY type parameter. If all parameters have
    // dataType ANY, then the resultType will be INT64[] (default type).
    auto childType = getValidLogicalType(arguments);
    if (childType.getLogicalTypeID() == LogicalTypeID::ANY) {
        childType = LogicalType(LogicalTypeID::STRING);
    }
    // Cast parameters with ANY dataType to resultChildType.
    for (auto& argument : arguments) {
        auto& parameterType = argument->getDataTypeReference();
        if (parameterType != childType) {
            if (parameterType.getLogicalTypeID() == LogicalTypeID::ANY) {
                argument->cast(childType);
            } else {
                throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(name,
                    arguments[0]->getDataType(), argument->getDataType()));
            }
        }
    }
    return childType;
}

function_set ListCreationFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::LIST, execFunc, nullptr, ListCreationBindFunc, true /*  isVarLength */));
    return result;
}

static std::unique_ptr<FunctionBindData> ListRangeBindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    KU_ASSERT(arguments[0]->dataType == arguments[1]->dataType);
    auto resultType =
        LogicalType::LIST(std::make_unique<LogicalType>(arguments[0]->dataType.getLogicalTypeID()));
    return std::make_unique<FunctionBindData>(std::move(resultType));
}

function_set ListRangeFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getIntegerLogicalTypeIDs()) {
        // start, end
        result.push_back(std::make_unique<ScalarFunction>(name,
            std::vector<LogicalTypeID>{typeID, typeID}, LogicalTypeID::LIST,
            getBinaryListExecFuncSwitchAll<Range, list_entry_t>(LogicalType{typeID}), nullptr,
            ListRangeBindFunc, false));
        // start, end, step
        result.push_back(std::make_unique<ScalarFunction>(name,
            std::vector<LogicalTypeID>{typeID, typeID, typeID}, LogicalTypeID::LIST,
            getTernaryListExecFuncSwitchAll<Range, list_entry_t>(LogicalType{typeID}), nullptr,
            ListRangeBindFunc, false));
    }
    return result;
}

function_set SizeFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<list_entry_t, int64_t, ListLen>, true /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(alias,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<list_entry_t, int64_t, ListLen>, true /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<ku_string_t, int64_t, ListLen>, true /* isVarlength*/));
    return result;
}

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
    return std::make_unique<FunctionBindData>(resultType->copy());
}

function_set ListExtractFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::INT64}, LogicalTypeID::ANY,
        nullptr, nullptr, ListExtractBindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryExecFunction<ku_string_t, int64_t, ku_string_t, ListExtract>,
        false /* isVarlength */));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ARRAY, LogicalTypeID::INT64}, LogicalTypeID::ANY,
        nullptr, nullptr, ListExtractBindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListConcatFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(name,
            arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType().copy());
}

function_set ListConcatFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, ListConcat>;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        execFunc, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

static std::unique_ptr<FunctionBindData> ListAppendBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    if (*ListType::getChildType(&arguments[0]->dataType) != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            ListAppendFunction::name, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[0]->getDataType();
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListAppend, list_entry_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(resultType.copy());
}

function_set ListAppendFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        nullptr, nullptr, ListAppendBindFunc, false /* isVarlength*/));
    return result;
}

static std::unique_ptr<FunctionBindData> ListPrependBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    if (arguments[0]->getDataType().getLogicalTypeID() != LogicalTypeID::ANY &&
        arguments[1]->dataType != *ListType::getChildType(&arguments[0]->dataType)) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            ListPrependFunction::name, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[0]->getDataType();
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListPrepend, list_entry_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(resultType.copy());
}

function_set ListPrependFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        nullptr, nullptr, ListPrependBindFunc, false /* isVarlength */));
    return result;
}

static std::unique_ptr<FunctionBindData> ListPositionBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListPosition, int64_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType::INT64());
}

function_set ListPositionFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::INT64,
        nullptr, nullptr, ListPositionBindFunc, false /* isVarlength */));
    return result;
}

static std::unique_ptr<FunctionBindData> ListContainsBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListContains, uint8_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType::BOOL());
}

function_set ListContainsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::BOOL,
        nullptr, nullptr, ListContainsBindFunc, false /* isVarlength */));
    return result;
}

static std::unique_ptr<FunctionBindData> ListSliceBindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType().copy());
}

function_set ListSliceFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::LIST,
        ScalarFunction::TernaryExecListStructFunction<list_entry_t, int64_t, int64_t, list_entry_t,
            ListSlice>,
        nullptr, ListSliceBindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
            LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryExecListStructFunction<ku_string_t, int64_t, int64_t, ku_string_t,
            ListSlice>,
        false /* isVarlength */));
    return result;
}

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

static std::unique_ptr<FunctionBindData> ListSortBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
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
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType().copy());
}

function_set ListSortFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::LIST, nullptr, nullptr, ListSortBindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING}, LogicalTypeID::LIST,
        nullptr, nullptr, ListSortBindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::LIST, nullptr, nullptr, ListSortBindFunc, false /* isVarlength*/));
    return result;
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
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType().copy());
}

function_set ListReverseSortFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::LIST, nullptr, nullptr,
        ListReverseSortBindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING}, LogicalTypeID::LIST,
        nullptr, nullptr, ListReverseSortBindFunc, false /* isVarlength*/));
    return result;
}

function_set ListSumFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        bindFuncListAggr<ListSum>, false /* isVarlength*/));
    return result;
}

function_set ListProductFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        bindFuncListAggr<ListProduct>, false /* isVarlength*/));
    return result;
}

static std::unique_ptr<FunctionBindData> ListDistinctBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    switch (ListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<int16_t>>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<int8_t>>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<uint64_t>>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<uint32_t>>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<uint16_t>>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<uint8_t>>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<int128_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<double>>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<float>>;
    } break;
    case LogicalTypeID::BOOL: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<timestamp_ms_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<timestamp_ns_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<timestamp_sec_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<timestamp_tz_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<timestamp_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            list_entry_t, ListDistinct<internalID_t>>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType().copy());
}

function_set ListDistinctFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::LIST, nullptr, nullptr, ListDistinctBindFunc, false /* isVarlength*/));
    return result;
}

static std::unique_ptr<FunctionBindData> ListUniqueBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    switch (ListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<int16_t>>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<int8_t>>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<uint64_t>>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<uint32_t>>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<uint16_t>>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<uint8_t>>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<int128_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<double>>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<float>>;
    } break;
    case LogicalTypeID::BOOL: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<timestamp_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<timestamp_ms_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<timestamp_ns_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<timestamp_sec_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<timestamp_tz_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc = ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t,
            int64_t, ListUnique<internalID_t>>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(LogicalType::INT64());
}

function_set ListUniqueFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::INT64, nullptr, nullptr, ListUniqueBindFunc, false /* isVarlength*/));
    return result;
}

static std::unique_ptr<FunctionBindData> ListAnyValueBindFunc(
    const binder::expression_vector& arguments, Function* function) {
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
    return std::make_unique<FunctionBindData>(resultType->copy());
}

function_set ListAnyValueFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::ANY, nullptr, nullptr, ListAnyValueBindFunc, false /* isVarlength*/));
    return result;
}

static std::unique_ptr<FunctionBindData> ListReverseBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    auto resultType = arguments[0]->dataType;
    scalarFunction->execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, ListReverse>;
    return std::make_unique<FunctionBindData>(resultType.copy());
}

function_set ListReverseFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::ANY, nullptr, nullptr, ListReverseBindFunc, false /* isVarlength*/));
    return result;
}

} // namespace function
} // namespace kuzu
