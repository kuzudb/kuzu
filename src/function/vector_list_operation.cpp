#include "common/types/ku_list.h"
#include "function/list/operations/list_append_operation.h"
#include "function/list/operations/list_concat_operation.h"
#include "function/list/operations/list_contains.h"
#include "function/list/operations/list_extract_operation.h"
#include "function/list/operations/list_len_operation.h"
#include "function/list/operations/list_position_operation.h"
#include "function/list/operations/list_prepend_operation.h"
#include "function/list/operations/list_slice_operation.h"
#include "function/list/vector_list_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::string getListFunctionIncompatibleChildrenTypeErrorMsg(
    const std::string& functionName, const DataType& left, const DataType& right) {
    return std::string("Cannot bind " + functionName + " with parameter type " +
                       Types::dataTypeToString(left) + " and " + Types::dataTypeToString(right) +
                       ".");
}

void VectorListOperations::ListCreation(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    assert(!parameters.empty() && result.dataType.typeID == VAR_LIST);
    result.resetOverflowBuffer();
    auto& childType = parameters[0]->dataType;
    auto numBytesOfListElement = Types::getDataTypeSize(childType);
    auto elements = std::make_unique<uint8_t[]>(parameters.size() * numBytesOfListElement);
    for (auto selectedPos = 0u; selectedPos < result.state->selVector->selectedSize;
         ++selectedPos) {
        auto pos = result.state->selVector->selectedPositions[selectedPos];
        auto& kuList = ((ku_list_t*)result.getData())[pos];
        for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
            auto paramPos = parameters[paramIdx]->state->isFlat() ?
                                parameters[paramIdx]->state->selVector->selectedPositions[0] :
                                pos;
            memcpy(elements.get() + paramIdx * numBytesOfListElement,
                parameters[paramIdx]->getData() + paramPos * numBytesOfListElement,
                numBytesOfListElement);
        }
        ku_list_t tmpList(parameters.size(), (uint64_t)elements.get());
        InMemOverflowBufferUtils::copyListRecursiveIfNested(
            tmpList, kuList, result.dataType, result.getOverflowBuffer());
    }
}

void ListCreationVectorOperation::listCreationBindFunc(
    const binder::expression_vector& argumentTypes, FunctionDefinition* definition,
    DataType& actualReturnType) {
    if (argumentTypes.empty()) {
        throw BinderException(
            "Cannot resolve child data type for " + LIST_CREATION_FUNC_NAME + ".");
    }
    // TODO(Ziyi): Support list of structs.
    if (argumentTypes[0]->getDataType().getTypeID() == common::STRUCT) {
        throw BinderException("Cannot create a list of structs.");
    }
    for (auto i = 1u; i < argumentTypes.size(); i++) {
        if (argumentTypes[i]->getDataType() != argumentTypes[0]->getDataType()) {
            throw BinderException(
                getListFunctionIncompatibleChildrenTypeErrorMsg(LIST_CREATION_FUNC_NAME,
                    argumentTypes[0]->getDataType(), argumentTypes[i]->getDataType()));
        }
    }
    definition->returnTypeID = VAR_LIST;
    actualReturnType =
        DataType(VAR_LIST, std::make_unique<DataType>(argumentTypes[0]->getDataType()));
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListCreationVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CREATION_FUNC_NAME,
        std::vector<DataTypeID>{ANY}, VAR_LIST, ListCreation, nullptr, listCreationBindFunc,
        true /*  isVarLength */));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ListLenVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    auto execFunc = UnaryExecFunction<ku_list_t, int64_t, operation::ListLen>;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_LEN_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST}, INT64, execFunc, true /* isVarlength*/));
    return result;
}

void ListExtractVectorOperation::listExtractBindFunc(const binder::expression_vector& argumentTypes,
    FunctionDefinition* definition, DataType& returnType) {
    definition->returnTypeID = argumentTypes[0]->getDataType().getChildType()->typeID;
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    returnType = *argumentTypes[0]->getDataType().getChildType();
    switch (definition->returnTypeID) {
    case BOOL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, uint8_t, operation::ListExtract>;
    } break;
    case INT64: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, int64_t, operation::ListExtract>;
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, double_t, operation::ListExtract>;
    } break;
    case DATE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, date_t, operation::ListExtract>;
    } break;
    case TIMESTAMP: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, timestamp_t, operation::ListExtract>;
    } break;
    case INTERVAL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, interval_t, operation::ListExtract>;
    } break;
    case STRING: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, ku_string_t, operation::ListExtract>;
    } break;
    case VAR_LIST: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, ku_list_t, operation::ListExtract>;
    } break;
    default: {
        assert(false);
    }
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListExtractVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, INT64}, ANY, nullptr, nullptr, listExtractBindFunc,
        false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64}, STRING,
        BinaryExecFunction<ku_string_t, int64_t, ku_string_t, operation::ListExtract>,
        false /* isVarlength */));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListConcatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    auto execFunc = BinaryListExecFunction<ku_list_t, ku_list_t, ku_list_t, operation::ListConcat>;
    auto bindFunc = [](const binder::expression_vector& argumentTypes,
                        FunctionDefinition* definition, DataType& actualReturnType) {
        if (argumentTypes[0]->getDataType() != argumentTypes[1]->getDataType()) {
            throw BinderException(
                getListFunctionIncompatibleChildrenTypeErrorMsg(LIST_CONCAT_FUNC_NAME,
                    argumentTypes[0]->getDataType(), argumentTypes[1]->getDataType()));
        }
        definition->returnTypeID = argumentTypes[0]->getDataType().getTypeID();
        actualReturnType = argumentTypes[0]->getDataType();
    };
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CONCAT_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, VAR_LIST}, VAR_LIST, execFunc, nullptr, bindFunc,
        false /* isVarlength*/));
    return result;
}

void ListAppendVectorOperation::listAppendBindFunc(const binder::expression_vector& argumentTypes,
    FunctionDefinition* definition, DataType& returnType) {
    if (*argumentTypes[0]->getDataType().getChildType() != argumentTypes[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(LIST_APPEND_FUNC_NAME,
            argumentTypes[0]->getDataType(), argumentTypes[1]->getDataType()));
    }
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    definition->returnTypeID = argumentTypes[0]->getDataType().typeID;
    returnType = argumentTypes[0]->getDataType();
    switch (argumentTypes[1]->getDataType().typeID) {
    case INT64: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, int64_t, ku_list_t, operation::ListAppend>;
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, double_t, ku_list_t, operation::ListAppend>;
    } break;
    case BOOL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, uint8_t, ku_list_t, operation::ListAppend>;
    } break;
    case STRING: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, ku_string_t, ku_list_t, operation::ListAppend>;
    } break;
    case DATE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, date_t, ku_list_t, operation::ListAppend>;
    } break;
    case TIMESTAMP: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, timestamp_t, ku_list_t, operation::ListAppend>;
    } break;
    case INTERVAL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, interval_t, ku_list_t, operation::ListAppend>;
    } break;
    case VAR_LIST: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, ku_list_t, ku_list_t, operation::ListAppend>;
    } break;
    default: {
        assert(false);
    }
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListAppendVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_APPEND_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, ANY}, VAR_LIST, nullptr, nullptr, listAppendBindFunc,
        false /* isVarlength*/));
    return result;
}

void ListPrependVectorOperation::listPrependBindFunc(const binder::expression_vector& argumentTypes,
    FunctionDefinition* definition, DataType& returnType) {
    if (argumentTypes[0]->dataType != *argumentTypes[1]->dataType.getChildType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(LIST_APPEND_FUNC_NAME,
            argumentTypes[0]->getDataType(), argumentTypes[1]->getDataType()));
    }
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    definition->returnTypeID = argumentTypes[1]->getDataType().typeID;
    returnType = argumentTypes[1]->getDataType();
    switch (argumentTypes[0]->getDataType().getTypeID()) {
    case INT64: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<int64_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<double_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    case BOOL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<uint8_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    case STRING: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_string_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    case DATE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<date_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    case TIMESTAMP: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<timestamp_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    case INTERVAL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<interval_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    case VAR_LIST: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_list_t, ku_list_t, ku_list_t, operation::ListPrepend>;
    } break;
    default: {
        assert(false);
    }
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListPrependVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_PREPEND_FUNC_NAME,
        std::vector<DataTypeID>{ANY, VAR_LIST}, VAR_LIST, nullptr, nullptr, listPrependBindFunc,
        false /* isVarlength*/));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListPositionVectorOperation::getDefinitions() {
    return getBinaryListOperationDefinitions<operation::ListPosition, int64_t>(
        LIST_POSITION_FUNC_NAME, INT64);
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListContainsVectorOperation::getDefinitions() {
    return getBinaryListOperationDefinitions<operation::ListContains, uint8_t>(
        LIST_CONTAINS_FUNC_NAME, BOOL);
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ListSliceVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    auto bindFunc = [](const binder::expression_vector& argumentTypes,
                        FunctionDefinition* definition, DataType& actualReturnType) {
        definition->returnTypeID = argumentTypes[0]->getDataType().typeID;
        actualReturnType = argumentTypes[0]->getDataType();
    };
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, INT64, INT64}, VAR_LIST,
        TernaryListExecFunction<ku_list_t, int64_t, int64_t, ku_list_t, operation::ListSlice>,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64, INT64}, STRING,
        TernaryListExecFunction<ku_string_t, int64_t, int64_t, ku_string_t, operation::ListSlice>,
        false /* isVarlength */));
    return result;
}

} // namespace function
} // namespace kuzu
