#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "function/list/functions/list_append_function.h"
#include "function/list/functions/list_contains_function.h"
#include "function/list/functions/list_position_function.h"
#include "function/list/functions/list_prepend_function.h"
#include "function/list/vector_list_functions.h"
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
    case PhysicalTypeID::ARRAY:
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

static std::unique_ptr<FunctionBindData> ListAppendBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    if (*ListType::getChildType(&arguments[0]->dataType) != arguments[1]->getDataType()) {
        throw BinderException(
            ExceptionMessage::listFunctionIncompatibleChildrenType(ListAppendFunction::name,
                arguments[0]->getDataType().toString(), arguments[1]->getDataType().toString()));
    }
    auto resultType = arguments[0]->getDataType();
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListAppend, list_entry_t>(arguments[1]->getDataType());
    return FunctionBindData::getSimpleBindData(arguments, resultType);
}

function_set ListAppendFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        nullptr, nullptr, ListAppendBindFunc));
    return result;
}

static std::unique_ptr<FunctionBindData> ListPrependBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    if (arguments[0]->getDataType().getLogicalTypeID() != LogicalTypeID::ANY &&
        arguments[1]->dataType != *ListType::getChildType(&arguments[0]->dataType)) {
        throw BinderException(
            ExceptionMessage::listFunctionIncompatibleChildrenType(ListPrependFunction::name,
                arguments[0]->getDataType().toString(), arguments[1]->getDataType().toString()));
    }
    auto resultType = arguments[0]->getDataType();
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListPrepend, list_entry_t>(arguments[1]->getDataType());
    return FunctionBindData::getSimpleBindData(arguments, resultType);
}

function_set ListPrependFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        nullptr, nullptr, ListPrependBindFunc));
    return result;
}

static std::unique_ptr<FunctionBindData> ListPositionBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListPosition, int64_t>(arguments[1]->getDataType());
    return FunctionBindData::getSimpleBindData(arguments, *LogicalType::INT64());
}

function_set ListPositionFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::INT64,
        nullptr, nullptr, ListPositionBindFunc));
    return result;
}

static std::unique_ptr<FunctionBindData> ListContainsBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = function->ptrCast<ScalarFunction>();
    scalarFunction->execFunc =
        getBinaryListExecFuncSwitchRight<ListContains, uint8_t>(arguments[1]->getDataType());
    return FunctionBindData::getSimpleBindData(arguments, *LogicalType::BOOL());
}

function_set ListContainsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::BOOL,
        nullptr, nullptr, ListContainsBindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
