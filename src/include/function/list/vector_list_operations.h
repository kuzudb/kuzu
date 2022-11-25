#pragma once

#include "function/vector_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct VectorListOperations : public VectorOperations {

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryListExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 3);
        TernaryOperationExecutor::executeStringAndList<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryListPosAndContainsExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeListPosAndContains<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE,
            FUNC>(*params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryListExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeStringAndList<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    static void ListCreation(
        const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result);

    template<typename OPERATION, typename RESULT_TYPE>
    static vector<unique_ptr<VectorOperationDefinition>> getBinaryListOperationDefinitions(
        string funcName, DataTypeID resultTypeID) {
        vector<unique_ptr<VectorOperationDefinition>> result;
        scalar_exec_func execFunc;
        for (auto& rightTypeID :
            vector<DataTypeID>{BOOL, INT64, DOUBLE, STRING, DATE, TIMESTAMP, INTERVAL, LIST}) {
            switch (rightTypeID) {
            case BOOL: {
                execFunc = BinaryListPosAndContainsExecFunction<ku_list_t, uint8_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            case INT64: {
                execFunc = BinaryListPosAndContainsExecFunction<ku_list_t, int64_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            case DOUBLE: {
                execFunc = BinaryListPosAndContainsExecFunction<ku_list_t, double_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            case STRING: {
                execFunc = BinaryListPosAndContainsExecFunction<ku_list_t, ku_string_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            case DATE: {
                execFunc =
                    BinaryListPosAndContainsExecFunction<ku_list_t, date_t, RESULT_TYPE, OPERATION>;
            } break;
            case TIMESTAMP: {
                execFunc = BinaryListPosAndContainsExecFunction<ku_list_t, timestamp_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            case INTERVAL: {
                execFunc = BinaryListPosAndContainsExecFunction<ku_list_t, interval_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            case LIST: {
                execFunc = BinaryListPosAndContainsExecFunction<ku_list_t, ku_list_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            default: {
                assert(false);
            }
            }
            result.push_back(make_unique<VectorOperationDefinition>(funcName,
                vector<DataTypeID>{LIST, rightTypeID}, resultTypeID, execFunc, nullptr,
                false /* isVarlength*/));
        }
        return result;
    }
};

struct ListCreationVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
    static void listCreationBindFunc(const vector<DataType>& argumentTypes,
        VectorOperationDefinition* definition, DataType& actualReturnType);
};

struct ListLenVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListExtractVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
    static void listExtractBindFunc(const vector<DataType>& argumentTypes,
        VectorOperationDefinition* definition, DataType& returnType);
};

struct ListConcatVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListAppendVectorOperation : public VectorListOperations {
    static void listAppendBindFunc(const vector<DataType>& argumentTypes,
        VectorOperationDefinition* definition, DataType& returnType);
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListPrependVectorOperation : public VectorListOperations {
    static void listPrependBindFunc(const vector<DataType>& argumentTypes,
        VectorOperationDefinition* definition, DataType& returnType);
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListPositionVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListContainsVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListSliceVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace kuzu
