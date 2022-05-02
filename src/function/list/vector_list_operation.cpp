#include "include/vector_list_operations.h"

#include "src/common/types/include/gf_list.h"
#include "src/function/list/operations/include/list_append_operation.h"
#include "src/function/list/operations/include/list_concat_operation.h"
#include "src/function/list/operations/include/list_contains.h"
#include "src/function/list/operations/include/list_extract_operation.h"
#include "src/function/list/operations/include/list_len_operation.h"
#include "src/function/list/operations/include/list_position_operation.h"
#include "src/function/list/operations/include/list_prepend_operation.h"
#include "src/function/list/operations/include/list_slice_operation.h"

namespace graphflow {
namespace function {

void VectorListOperations::ListCreation(
    const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    result.resetOverflowBuffer();
    assert(!parameters.empty() && result.dataType.typeID == LIST);
    auto& childType = parameters[0]->dataType;
    auto numBytesOfListElement = Types::getDataTypeSize(childType);
    vector<uint8_t*> listElements(parameters.size());
    if (result.state->isFlat()) {
        auto pos = result.state->getPositionOfCurrIdx();
        auto& gfList = ((gf_list_t*)result.values)[pos];
        for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
            assert(parameters[paramIdx]->state->isFlat());
            listElements[paramIdx] = parameters[paramIdx]->values + pos * numBytesOfListElement;
        }
        TypeUtils::copyListNonRecursive(
            listElements, gfList, result.dataType, result.getOverflowBuffer());
    } else {
        for (auto selectedPos = 0u; selectedPos < result.state->selectedSize; ++selectedPos) {
            auto pos = result.state->selectedPositions[selectedPos];
            auto& gfList = ((gf_list_t*)result.values)[pos];
            for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
                auto parameterPos = parameters[paramIdx]->state->isFlat() ?
                                        parameters[paramIdx]->state->getPositionOfCurrIdx() :
                                        pos;
                listElements[paramIdx] =
                    parameters[paramIdx]->values + parameterPos * numBytesOfListElement;
            }
            TypeUtils::copyListNonRecursive(
                listElements, gfList, result.dataType, result.getOverflowBuffer());
        }
    }
}

void ListCreationVectorOperation::listCreationBindFunc(const vector<DataType>& argumentTypes,
    VectorOperationDefinition* definition, DataType& actualReturnType) {
    for (auto i = 1u; i < argumentTypes.size(); i++) {
        assert(argumentTypes[i] == argumentTypes[0]);
    }
    definition->returnTypeID = LIST;
    actualReturnType = DataType(LIST, make_unique<DataType>(argumentTypes[0]));
}

vector<unique_ptr<VectorOperationDefinition>> ListCreationVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(LIST_CREATION_FUNC_NAME, vector<DataTypeID>{ANY},
            LIST, ListCreation, nullptr, listCreationBindFunc, true /* isVarlength*/));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> ListLenVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    auto execFunc = UnaryExecFunction<gf_list_t, int64_t, operation::ListLen>;
    result.push_back(make_unique<VectorOperationDefinition>(
        LIST_LEN_FUNC_NAME, vector<DataTypeID>{LIST}, INT64, execFunc, true /* isVarlength*/));
    return result;
}

void ListExtractVectorOperation::listExtractBindFunc(const vector<DataType>& argumentTypes,
    VectorOperationDefinition* definition, DataType& returnType) {
    definition->returnTypeID = argumentTypes[0].childType->typeID;
    returnType = *argumentTypes[0].childType;
    switch (definition->returnTypeID) {
    case BOOL: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, uint8_t, operation::ListExtract>;
    } break;
    case INT64: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, int64_t, operation::ListExtract>;
    } break;
    case DOUBLE: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, double_t, operation::ListExtract>;
    } break;
    case DATE: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, date_t, operation::ListExtract>;
    } break;
    case TIMESTAMP: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, timestamp_t, operation::ListExtract>;
    } break;
    case INTERVAL: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, interval_t, operation::ListExtract>;
    } break;
    case STRING: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, gf_string_t, operation::ListExtract>;
    } break;
    case LIST: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, gf_list_t, operation::ListExtract>;
    } break;
    default: {
        assert(false);
    }
    }
}

vector<unique_ptr<VectorOperationDefinition>> ListExtractVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
        vector<DataTypeID>{LIST, INT64}, ANY, nullptr, nullptr, listExtractBindFunc,
        false /* isVarlength*/));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> ListConcatVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    auto execFunc = BinaryListExecFunction<gf_list_t, gf_list_t, gf_list_t, operation::ListConcat>;
    auto bindFunc = [](const vector<DataType>& argumentTypes, VectorOperationDefinition* definition,
                        DataType& actualReturnType) {
        assert(argumentTypes[0] == argumentTypes[1]);
        definition->returnTypeID = argumentTypes[0].typeID;
        actualReturnType = argumentTypes[0];
    };
    result.push_back(make_unique<VectorOperationDefinition>(LIST_CONCAT_FUNC_NAME,
        vector<DataTypeID>{LIST, LIST}, LIST, execFunc, nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

void ListAppendVectorOperation::listAppendBindFunc(const vector<DataType>& argumentTypes,
    VectorOperationDefinition* definition, DataType& returnType) {
    assert(*argumentTypes[0].childType == argumentTypes[1]);
    definition->returnTypeID = argumentTypes[0].typeID;
    returnType = argumentTypes[0];
    switch (argumentTypes[1].typeID) {
    case INT64: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, int64_t, gf_list_t, operation::ListAppend>;
    } break;
    case DOUBLE: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, double_t, gf_list_t, operation::ListAppend>;
    } break;
    case BOOL: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, uint8_t, gf_list_t, operation::ListAppend>;
    } break;
    case STRING: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, gf_string_t, gf_list_t, operation::ListAppend>;
    } break;
    case DATE: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, date_t, gf_list_t, operation::ListAppend>;
    } break;
    case TIMESTAMP: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, timestamp_t, gf_list_t, operation::ListAppend>;
    } break;
    case INTERVAL: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, interval_t, gf_list_t, operation::ListAppend>;
    } break;
    case LIST: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, gf_list_t, gf_list_t, operation::ListAppend>;
    } break;
    default: {
        assert(false);
    }
    }
}

vector<unique_ptr<VectorOperationDefinition>> ListAppendVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(LIST_APPEND_FUNC_NAME, vector<DataTypeID>{LIST, ANY},
            LIST, nullptr, nullptr, listAppendBindFunc, false /* isVarlength*/));
    return result;
}

void ListPrependVectorOperation::listPrependBindFunc(const vector<DataType>& argumentTypes,
    VectorOperationDefinition* definition, DataType& returnType) {
    assert(argumentTypes[0] == *argumentTypes[1].childType);
    definition->returnTypeID = argumentTypes[1].typeID;
    returnType = argumentTypes[1];
    switch (argumentTypes[0].typeID) {
    case INT64: {
        definition->execFunc =
            BinaryListExecFunction<int64_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    case DOUBLE: {
        definition->execFunc =
            BinaryListExecFunction<double_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    case BOOL: {
        definition->execFunc =
            BinaryListExecFunction<uint8_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    case STRING: {
        definition->execFunc =
            BinaryListExecFunction<gf_string_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    case DATE: {
        definition->execFunc =
            BinaryListExecFunction<date_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    case TIMESTAMP: {
        definition->execFunc =
            BinaryListExecFunction<timestamp_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    case INTERVAL: {
        definition->execFunc =
            BinaryListExecFunction<interval_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    case LIST: {
        definition->execFunc =
            BinaryListExecFunction<gf_list_t, gf_list_t, gf_list_t, operation::ListPrepend>;
    } break;
    default: {
        assert(false);
    }
    }
}

vector<unique_ptr<VectorOperationDefinition>> ListPrependVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(LIST_PREPEND_FUNC_NAME,
        vector<DataTypeID>{ANY, LIST}, LIST, nullptr, nullptr, listPrependBindFunc,
        false /* isVarlength*/));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> ListPositionVectorOperation::getDefinitions() {
    return getBinaryListOperationDefinitions<operation::ListPosition, int64_t>(
        LIST_POSITION_FUNC_NAME, INT64);
}

vector<unique_ptr<VectorOperationDefinition>> ListContainsVectorOperation::getDefinitions() {
    return getBinaryListOperationDefinitions<operation::ListContains, uint8_t>(
        LIST_CONTAINS_FUNC_NAME, BOOL);
}

vector<unique_ptr<VectorOperationDefinition>> ListSliceVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    auto bindFunc = [](const vector<DataType>& argumentTypes, VectorOperationDefinition* definition,
                        DataType& actualReturnType) {
        definition->returnTypeID = argumentTypes[0].typeID;
        actualReturnType = argumentTypes[0];
    };
    result.push_back(make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
        vector<DataTypeID>{LIST, INT64, INT64}, LIST,
        TernaryListExecFunction<gf_list_t, int64_t, int64_t, gf_list_t, operation::ListSlice>,
        nullptr, bindFunc, false /* isVarlength*/));
    return result;
}

} // namespace function
} // namespace graphflow
