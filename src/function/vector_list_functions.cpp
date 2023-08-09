#include "function/list/vector_list_functions.h"

#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/types/ku_list.h"
#include "function/list/functions/list_any_value_function.h"
#include "function/list/functions/list_append_function.h"
#include "function/list/functions/list_concat_function.h"
#include "function/list/functions/list_contains_function.h"
#include "function/list/functions/list_distinct_function.h"
#include "function/list/functions/list_extract_function.h"
#include "function/list/functions/list_len_function.h"
#include "function/list/functions/list_position_function.h"
#include "function/list/functions/list_prepend_function.h"
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
    return std::string("Cannot bind " + functionName + " with parameter type " +
                       LogicalTypeUtils::dataTypeToString(left) + " and " +
                       LogicalTypeUtils::dataTypeToString(right) + ".");
}

void ListCreationVectorFunction::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    assert(result.dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST);
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

std::unique_ptr<FunctionBindData> ListCreationVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    // ListCreation requires all parameters to have the same type or be ANY type. The result type of
    // listCreation can be determined by the first non-ANY type parameter. If all parameters have
    // dataType ANY, then the resultType will be INT64[] (default type).
    auto varListTypeInfo =
        std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    auto resultType = LogicalType{LogicalTypeID::VAR_LIST, std::move(varListTypeInfo)};
    for (auto& argument : arguments) {
        if (argument->getDataType().getLogicalTypeID() != LogicalTypeID::ANY) {
            varListTypeInfo = std::make_unique<VarListTypeInfo>(
                std::make_unique<LogicalType>(argument->getDataType()));
            resultType = LogicalType{LogicalTypeID::VAR_LIST, std::move(varListTypeInfo)};
            break;
        }
    }
    auto resultChildType = VarListType::getChildType(&resultType);
    // Cast parameters with ANY dataType to resultChildType.
    for (auto& argument : arguments) {
        auto parameterType = argument->getDataType();
        if (parameterType != *resultChildType) {
            if (parameterType.getLogicalTypeID() == LogicalTypeID::ANY) {
                binder::ExpressionBinder::resolveAnyDataType(*argument, *resultChildType);
            } else {
                throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
                    LIST_CREATION_FUNC_NAME, arguments[0]->getDataType(), argument->getDataType()));
            }
        }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

vector_function_definitions ListCreationVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_CREATION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST, execFunc, nullptr,
        bindFunc, true /*  isVarLength */));
    return result;
}

vector_function_definitions ListLenVectorFunction::getDefinitions() {
    vector_function_definitions result;
    auto execFunc = UnaryExecFunction<list_entry_t, int64_t, ListLen>;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_LEN_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, execFunc,
        true /* isVarlength*/));
    result.push_back(std::make_unique<VectorFunctionDefinition>(CARDINALITY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::INT64, execFunc,
        true /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListExtractVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    switch (resultType->getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, uint8_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT64: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, int64_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT32: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, int32_t, ListExtract>;
    } break;
    case PhysicalTypeID::INT16: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, int16_t, ListExtract>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, double_t, ListExtract>;
    } break;
    case PhysicalTypeID::FLOAT: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, float_t, ListExtract>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, interval_t, ListExtract>;
    } break;
    case PhysicalTypeID::STRING: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, ku_string_t, ListExtract>;
    } break;
    case PhysicalTypeID::VAR_LIST: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, list_entry_t, ListExtract>;
    } break;
    case PhysicalTypeID::STRUCT: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, struct_entry_t, ListExtract>;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, int64_t, internalID_t, ListExtract>;
    } break;
    default: {
        throw NotImplementedException("ListExtractVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(*resultType);
}

vector_function_definitions ListExtractVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::INT64},
        LogicalTypeID::ANY, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, BinaryExecFunction<ku_string_t, int64_t, ku_string_t, ListExtract>,
        false /* isVarlength */));
    return result;
}

vector_function_definitions ListConcatVectorFunction::getDefinitions() {
    vector_function_definitions result;
    auto execFunc =
        BinaryExecListStructFunction<list_entry_t, list_entry_t, list_entry_t, ListConcat>;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_CONCAT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::VAR_LIST},
        LogicalTypeID::VAR_LIST, execFunc, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListConcatVectorFunction::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_CONCAT_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

std::unique_ptr<FunctionBindData> ListAppendVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (*VarListType::getChildType(&arguments[0]->dataType) != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[0]->getDataType();
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    vectorFunctionDefinition->execFunc =
        getBinaryListExecFunc<ListAppend, list_entry_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(resultType);
}

vector_function_definitions ListAppendVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_APPEND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListPrependVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (arguments[0]->getDataType().getLogicalTypeID() != LogicalTypeID::ANY &&
        arguments[0]->dataType != *VarListType::getChildType(&arguments[1]->dataType)) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[1]->getDataType();
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    switch (arguments[0]->getDataType().getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<int64_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::INT32: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<int32_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::INT16: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<int16_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<double_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::FLOAT: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<float_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::BOOL: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<uint8_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::STRING: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<ku_string_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<interval_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::VAR_LIST: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<list_entry_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        vectorFunctionDefinition->execFunc =
            BinaryExecListStructFunction<internalID_t, list_entry_t, list_entry_t, ListPrepend>;
    } break;
    default: {
        throw NotImplementedException("ListPrependVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

vector_function_definitions ListPrependVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_PREPEND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY, LogicalTypeID::VAR_LIST},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

vector_function_definitions ListPositionVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_POSITION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::INT64, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListPositionVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    vectorFunctionDefinition->execFunc =
        getBinaryListExecFunc<ListPosition, int64_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType{LogicalTypeID::INT64});
}

vector_function_definitions ListContainsVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_CONTAINS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::BOOL, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListContainsVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    vectorFunctionDefinition->execFunc =
        getBinaryListExecFunc<ListContains, uint8_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType{LogicalTypeID::BOOL});
}

vector_function_definitions ListSliceVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::VAR_LIST, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::VAR_LIST,
        TernaryExecListStructFunction<list_entry_t, int64_t, int64_t, list_entry_t, ListSlice>,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        TernaryExecListStructFunction<ku_string_t, int64_t, int64_t, ku_string_t, ListSlice>,
        false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListSliceVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

vector_function_definitions ListSortVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::VAR_LIST, LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListSortVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getExecFunction<int64_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getExecFunction<int32_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getExecFunction<int16_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getExecFunction<double_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getExecFunction<float_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getExecFunction<uint8_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getExecFunction<ku_string_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getExecFunction<date_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getExecFunction<timestamp_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getExecFunction<interval_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    default: {
        throw NotImplementedException("ListSortVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

template<typename T>
void ListSortVectorFunction::getExecFunction(
    const binder::expression_vector& arguments, scalar_exec_func& func) {
    if (arguments.size() == 1) {
        func = UnaryExecListStructFunction<list_entry_t, list_entry_t, ListSort<T>>;
        return;
    } else if (arguments.size() == 2) {
        func = BinaryExecListStructFunction<list_entry_t, ku_string_t, list_entry_t, ListSort<T>>;
        return;
    } else if (arguments.size() == 3) {
        func = TernaryExecListStructFunction<list_entry_t, ku_string_t, ku_string_t, list_entry_t,
            ListSort<T>>;
        return;
    } else {
        throw RuntimeException("Invalid number of arguments");
    }
}

vector_function_definitions ListReverseSortVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_REVERSE_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_REVERSE_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListReverseSortVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getExecFunction<int64_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getExecFunction<int32_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getExecFunction<int16_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getExecFunction<double_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getExecFunction<float_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getExecFunction<uint8_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getExecFunction<ku_string_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getExecFunction<date_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getExecFunction<timestamp_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getExecFunction<interval_t>(arguments, vectorFunctionDefinition->execFunc);
    } break;
    default: {
        throw NotImplementedException("ListReverseSortVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

template<typename T>
void ListReverseSortVectorFunction::getExecFunction(
    const binder::expression_vector& arguments, scalar_exec_func& func) {
    if (arguments.size() == 1) {
        func = UnaryExecListStructFunction<list_entry_t, list_entry_t, ListReverseSort<T>>;
        return;
    } else if (arguments.size() == 2) {
        func = BinaryExecListStructFunction<list_entry_t, ku_string_t, list_entry_t,
            ListReverseSort<T>>;
        return;
    } else {
        throw RuntimeException("Invalid number of arguments");
    }
}

vector_function_definitions ListSumVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_SUM_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListSumVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListSum>;
    } break;
    case LogicalTypeID::INT32: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int32_t, ListSum>;
    } break;
    case LogicalTypeID::INT16: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int16_t, ListSum>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, double_t, ListSum>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, float_t, ListSum>;
    } break;
    default: {
        throw NotImplementedException("ListSumVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(*resultType);
}

vector_function_definitions ListDistinctVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_DISTINCT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListDistinctVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<int16_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<double_t>>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<float_t>>;
    } break;
    case LogicalTypeID::BOOL: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<timestamp_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListDistinct<internalID_t>>;
    } break;
    default: {
        throw NotImplementedException("ListDistinctVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

vector_function_definitions ListUniqueVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_UNIQUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListUniqueVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<int16_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<double_t>>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<float_t>>;
    } break;
    case LogicalTypeID::BOOL: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<timestamp_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListUnique<internalID_t>>;
    } break;
    default: {
        throw NotImplementedException("ListUniqueVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(LogicalType(LogicalTypeID::INT64));
}

vector_function_definitions ListAnyValueVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(std::make_unique<VectorFunctionDefinition>(LIST_ANY_VALUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::ANY, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListAnyValueVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT32: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int32_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INT16: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int16_t, ListAnyValue>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, double_t, ListAnyValue>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, float_t, ListAnyValue>;
    } break;
    case LogicalTypeID::BOOL: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, uint8_t, ListAnyValue>;
    } break;
    case LogicalTypeID::STRING: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, ku_string_t, ListAnyValue>;
    } break;
    case LogicalTypeID::DATE: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, date_t, ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, timestamp_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INTERVAL: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, interval_t, ListAnyValue>;
    } break;
    case LogicalTypeID::VAR_LIST: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, ListAnyValue>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        vectorFunctionDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, internalID_t, ListAnyValue>;
    } break;
    default: {
        throw NotImplementedException("ListAnyValueVectorFunction::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(*resultType);
}

} // namespace function
} // namespace kuzu
