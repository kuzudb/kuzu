#include "binder/expression_binder.h"
#include "common/types/ku_list.h"
#include "function/list/operations/list_any_value_operation.h"
#include "function/list/operations/list_append_operation.h"
#include "function/list/operations/list_concat_operation.h"
#include "function/list/operations/list_contains.h"
#include "function/list/operations/list_distinct_operation.h"
#include "function/list/operations/list_extract_operation.h"
#include "function/list/operations/list_len_operation.h"
#include "function/list/operations/list_position_operation.h"
#include "function/list/operations/list_prepend_operation.h"
#include "function/list/operations/list_reverse_sort_operation.h"
#include "function/list/operations/list_slice_operation.h"
#include "function/list/operations/list_sort_operation.h"
#include "function/list/operations/list_sum_operation.h"
#include "function/list/operations/list_unique_operation.h"
#include "function/list/vector_list_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::string getListFunctionIncompatibleChildrenTypeErrorMsg(
    const std::string& functionName, const LogicalType& left, const LogicalType& right) {
    return std::string("Cannot bind " + functionName + " with parameter type " +
                       LogicalTypeUtils::dataTypeToString(left) + " and " +
                       LogicalTypeUtils::dataTypeToString(right) + ".");
}

void ListCreationVectorOperation::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    assert(result.dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST);
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < result.state->selVector->selectedSize;
         ++selectedPos) {
        auto pos = result.state->selVector->selectedPositions[selectedPos];
        auto resultEntry = common::ListVector::addList(&result, parameters.size());
        result.setValue(pos, resultEntry);
        auto resultValues = common::ListVector::getListValues(&result, resultEntry);
        auto resultDataVector = common::ListVector::getDataVector(&result);
        auto numBytesPerValue = resultDataVector->getNumBytesPerValue();
        for (auto i = 0u; i < parameters.size(); i++) {
            auto parameter = parameters[i];
            auto paramPos = parameter->state->isFlat() ?
                                parameter->state->selVector->selectedPositions[0] :
                                pos;
            if (parameter->isNull(paramPos)) {
                resultDataVector->setNull(resultEntry.offset + i, true);
            } else {
                resultDataVector->copyFromVectorData(resultValues, parameter.get(),
                    parameter->getData() + parameter->getNumBytesPerValue() * paramPos);
            }
            resultValues += numBytesPerValue;
        }
    }
}

std::unique_ptr<FunctionBindData> ListCreationVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    // ListCreation requires all parameters to have the same type or be ANY type. The result type of
    // listCreation can be determined by the first non-ANY type parameter. If all parameters have
    // dataType ANY, then the resultType will be INT64[] (default type).
    auto varListTypeInfo =
        std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    auto resultType = LogicalType{LogicalTypeID::VAR_LIST, std::move(varListTypeInfo)};
    for (auto& argument : arguments) {
        if (argument->getDataType().getLogicalTypeID() != common::LogicalTypeID::ANY) {
            varListTypeInfo = std::make_unique<VarListTypeInfo>(
                std::make_unique<LogicalType>(argument->getDataType()));
            resultType = LogicalType{common::LogicalTypeID::VAR_LIST, std::move(varListTypeInfo)};
            break;
        }
    }
    auto resultChildType = VarListType::getChildType(&resultType);
    // Cast parameters with ANY dataType to resultChildType.
    for (auto& argument : arguments) {
        auto parameterType = argument->getDataType();
        if (parameterType != *resultChildType) {
            if (parameterType.getLogicalTypeID() == common::LogicalTypeID::ANY) {
                binder::ExpressionBinder::resolveAnyDataType(*argument, *resultChildType);
            } else {
                throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
                    LIST_CREATION_FUNC_NAME, arguments[0]->getDataType(), argument->getDataType()));
            }
        }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

vector_operation_definitions ListCreationVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CREATION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST, execFunc, nullptr,
        bindFunc, true /*  isVarLength */));
    return result;
}

vector_operation_definitions ListLenVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    auto execFunc = UnaryExecFunction<list_entry_t, int64_t, operation::ListLen>;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_LEN_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, execFunc,
        true /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(CARDINALITY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::INT64, execFunc,
        true /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListExtractVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (resultType->getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, uint8_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::INT64: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, int64_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::INT32: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, int32_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::INT16: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, int16_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, double_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::FLOAT: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, float_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, interval_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::STRING: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, ku_string_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::VAR_LIST: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, list_entry_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::STRUCT: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, struct_entry_t, operation::ListExtract>;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<common::list_entry_t,
            int64_t, internalID_t, operation::ListExtract>;
    } break;
    default: {
        throw common::NotImplementedException("ListExtractVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(*resultType);
}

vector_operation_definitions ListExtractVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::INT64},
        LogicalTypeID::ANY, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        BinaryExecFunction<ku_string_t, int64_t, ku_string_t, operation::ListExtract>,
        false /* isVarlength */));
    return result;
}

vector_operation_definitions ListConcatVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    auto execFunc = BinaryExecListStructFunction<list_entry_t, list_entry_t, list_entry_t,
        operation::ListConcat>;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CONCAT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::VAR_LIST},
        LogicalTypeID::VAR_LIST, execFunc, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListConcatVectorOperation::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_CONCAT_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

std::unique_ptr<FunctionBindData> ListAppendVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (*VarListType::getChildType(&arguments[0]->dataType) != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[0]->getDataType();
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    vectorOperationDefinition->execFunc =
        getBinaryListExecFunc<operation::ListAppend, list_entry_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(resultType);
}

vector_operation_definitions ListAppendVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_APPEND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListPrependVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (arguments[0]->dataType != *VarListType::getChildType(&arguments[1]->dataType)) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[1]->getDataType();
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (arguments[0]->getDataType().getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<int64_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::INT32: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<int32_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::INT16: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<int16_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<double_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::FLOAT: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<float_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::BOOL: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<uint8_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::STRING: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<ku_string_t,
            list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<interval_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::VAR_LIST: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<list_entry_t,
            list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        vectorOperationDefinition->execFunc = BinaryExecListStructFunction<internalID_t,
            list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    default: {
        throw common::NotImplementedException("ListPrependVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

vector_operation_definitions ListPrependVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_PREPEND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY, LogicalTypeID::VAR_LIST},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

vector_operation_definitions ListPositionVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_POSITION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::INT64, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListPositionVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    vectorOperationDefinition->execFunc =
        getBinaryListExecFunc<operation::ListPosition, int64_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType{common::LogicalTypeID::INT64});
}

vector_operation_definitions ListContainsVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CONTAINS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::ANY},
        LogicalTypeID::BOOL, nullptr, nullptr, bindFunc, false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListContainsVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    vectorOperationDefinition->execFunc =
        getBinaryListExecFunc<operation::ListContains, uint8_t>(arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType{common::LogicalTypeID::BOOL});
}

vector_operation_definitions ListSliceVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::VAR_LIST, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::VAR_LIST,
        TernaryExecListStructFunction<common::list_entry_t, int64_t, int64_t, common::list_entry_t,
            operation::ListSlice>,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        TernaryExecListStructFunction<common::ku_string_t, int64_t, int64_t, common::ku_string_t,
            operation::ListSlice>,
        false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListSliceVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

vector_operation_definitions ListSortVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::VAR_LIST, LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListSortVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getExecFunction<int64_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getExecFunction<int32_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getExecFunction<int16_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getExecFunction<double_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getExecFunction<float_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getExecFunction<uint8_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getExecFunction<ku_string_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getExecFunction<date_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getExecFunction<timestamp_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getExecFunction<interval_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    default: {
        throw common::NotImplementedException("ListSortVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

template<typename T>
void ListSortVectorOperation::getExecFunction(
    const binder::expression_vector& arguments, scalar_exec_func& func) {
    if (arguments.size() == 1) {
        func = UnaryExecListStructFunction<list_entry_t, list_entry_t, operation::ListSort<T>>;
        return;
    } else if (arguments.size() == 2) {
        func = BinaryExecListStructFunction<list_entry_t, ku_string_t, list_entry_t,
            operation::ListSort<T>>;
        return;
    } else if (arguments.size() == 3) {
        func = TernaryExecListStructFunction<list_entry_t, ku_string_t, ku_string_t, list_entry_t,
            operation::ListSort<T>>;
        return;
    } else {
        throw common::RuntimeException("Invalid number of arguments");
    }
}

vector_operation_definitions ListReverseSortVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_REVERSE_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_REVERSE_SORT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListReverseSortVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        getExecFunction<int64_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::INT32: {
        getExecFunction<int32_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::INT16: {
        getExecFunction<int16_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::DOUBLE: {
        getExecFunction<double_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::FLOAT: {
        getExecFunction<float_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::BOOL: {
        getExecFunction<uint8_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::STRING: {
        getExecFunction<ku_string_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::DATE: {
        getExecFunction<date_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        getExecFunction<timestamp_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    case LogicalTypeID::INTERVAL: {
        getExecFunction<interval_t>(arguments, vectorOperationDefinition->execFunc);
    } break;
    default: {
        throw common::NotImplementedException("ListReverseSortVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

template<typename T>
void ListReverseSortVectorOperation::getExecFunction(
    const binder::expression_vector& arguments, scalar_exec_func& func) {
    if (arguments.size() == 1) {
        func =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, operation::ListReverseSort<T>>;
        return;
    } else if (arguments.size() == 2) {
        func = BinaryExecListStructFunction<list_entry_t, ku_string_t, list_entry_t,
            operation::ListReverseSort<T>>;
        return;
    } else {
        throw common::RuntimeException("Invalid number of arguments");
    }
}

vector_operation_definitions ListSumVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SUM_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListSumVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListSum>;
    } break;
    case LogicalTypeID::INT32: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int32_t, operation::ListSum>;
    } break;
    case LogicalTypeID::INT16: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int16_t, operation::ListSum>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, double_t, operation::ListSum>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, float_t, operation::ListSum>;
    } break;
    default: {
        throw common::NotImplementedException("ListSumVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(*resultType);
}

vector_operation_definitions ListDistinctVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_DISTINCT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::VAR_LIST, nullptr,
        nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListDistinctVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<int16_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<double_t>>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<float_t>>;
    } break;
    case LogicalTypeID::BOOL: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<timestamp_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t,
            list_entry_t, operation::ListDistinct<internalID_t>>;
    } break;
    default: {
        throw common::NotImplementedException("ListDistinctVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

vector_operation_definitions ListUniqueVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_UNIQUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::INT64, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListUniqueVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (common::VarListType::getChildType(&arguments[0]->dataType)->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<int64_t>>;
    } break;
    case LogicalTypeID::INT32: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<int32_t>>;
    } break;
    case LogicalTypeID::INT16: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<int16_t>>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<double_t>>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<float_t>>;
    } break;
    case LogicalTypeID::BOOL: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<uint8_t>>;
    } break;
    case LogicalTypeID::STRING: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<ku_string_t>>;
    } break;
    case LogicalTypeID::DATE: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<date_t>>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<timestamp_t>>;
    } break;
    case LogicalTypeID::INTERVAL: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListUnique<interval_t>>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        vectorOperationDefinition->execFunc = UnaryExecListStructFunction<list_entry_t, int64_t,
            operation::ListUnique<common::internalID_t>>;
    } break;
    default: {
        throw common::NotImplementedException("ListUniqueVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(LogicalType(LogicalTypeID::INT64));
}

vector_operation_definitions ListAnyValueVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_ANY_VALUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::ANY, nullptr, nullptr,
        bindFunc, false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListAnyValueVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    auto resultType = VarListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int64_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::INT32: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int32_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::INT16: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, int16_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::DOUBLE: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, double_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::FLOAT: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, float_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::BOOL: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, uint8_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::STRING: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, ku_string_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::DATE: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, date_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, timestamp_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::INTERVAL: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, interval_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::VAR_LIST: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, list_entry_t, operation::ListAnyValue>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        vectorOperationDefinition->execFunc =
            UnaryExecListStructFunction<list_entry_t, internalID_t, operation::ListAnyValue>;
    } break;
    default: {
        throw common::NotImplementedException("ListAnyValueVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(*resultType);
}

} // namespace function
} // namespace kuzu
