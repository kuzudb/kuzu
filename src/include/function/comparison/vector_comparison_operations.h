#pragma once

#include "binder/expression/expression.h"
#include "comparison_operations.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

class VectorComparisonOperations : public VectorOperations {

protected:
    template<typename FUNC>
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions(const string& name) {
        vector<unique_ptr<VectorOperationDefinition>> definitions;
        for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
            for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
                definitions.push_back(getDefinition<FUNC>(name, leftTypeID, rightTypeID));
            }
        }
        for (auto& typeID : vector<DataTypeID>{BOOL, STRING, NODE_ID, DATE, TIMESTAMP, INTERVAL}) {
            definitions.push_back(getDefinition<FUNC>(name, typeID, typeID));
        }
        definitions.push_back(getDefinition<FUNC>(name, DATE, TIMESTAMP));
        definitions.push_back(getDefinition<FUNC>(name, TIMESTAMP, DATE));
        return definitions;
    }

private:
    template<typename FUNC>
    static inline unique_ptr<VectorOperationDefinition> getDefinition(
        const string& name, DataTypeID leftTypeID, DataTypeID rightTypeID) {
        auto execFunc = getExecFunc<FUNC>(leftTypeID, rightTypeID);
        auto selectFunc = getSelectFunc<FUNC>(leftTypeID, rightTypeID);
        return make_unique<VectorOperationDefinition>(
            name, vector<DataTypeID>{leftTypeID, rightTypeID}, BOOL, execFunc, selectFunc);
    }

    template<typename FUNC>
    static scalar_exec_func getExecFunc(DataTypeID leftTypeID, DataTypeID rightTypeID) {
        switch (leftTypeID) {
        case INT64: {
            switch (rightTypeID) {
            case INT64: {
                return BinaryExecFunction<int64_t, int64_t, uint8_t, FUNC>;
            }
            case DOUBLE: {
                return BinaryExecFunction<int64_t, double_t, uint8_t, FUNC>;
            }
            default:
                throw RuntimeException("Invalid input data types(" +
                                       Types::dataTypeToString(leftTypeID) + "," +
                                       Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case DOUBLE: {
            switch (rightTypeID) {
            case INT64: {
                return BinaryExecFunction<double_t, int64_t, uint8_t, FUNC>;
            }
            case DOUBLE: {
                return BinaryExecFunction<double_t, double_t, uint8_t, FUNC>;
            }
            default:
                throw RuntimeException("Invalid input data types(" +
                                       Types::dataTypeToString(leftTypeID) + "," +
                                       Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case BOOL: {
            assert(rightTypeID == BOOL);
            return BinaryExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
        }
        case STRING: {
            assert(rightTypeID == STRING);
            return BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, FUNC>;
        }
        case NODE_ID: {
            assert(rightTypeID == NODE_ID);
            return BinaryExecFunction<nodeID_t, nodeID_t, uint8_t, FUNC>;
        }
        case DATE: {
            switch (rightTypeID) {
            case DATE: {
                return BinaryExecFunction<date_t, date_t, uint8_t, FUNC>;
            }
            case TIMESTAMP: {
                return BinaryExecFunction<date_t, timestamp_t, uint8_t, FUNC>;
            }
            default:
                throw RuntimeException("Invalid input data types(" +
                                       Types::dataTypeToString(leftTypeID) + "," +
                                       Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case TIMESTAMP: {
            switch (rightTypeID) {
            case DATE: {
                return BinaryExecFunction<timestamp_t, date_t, uint8_t, FUNC>;
            }
            case TIMESTAMP: {
                return BinaryExecFunction<timestamp_t, timestamp_t, uint8_t, FUNC>;
            }
            default:
                throw RuntimeException("Invalid input data types(" +
                                       Types::dataTypeToString(leftTypeID) + "," +
                                       Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case INTERVAL: {
            assert(rightTypeID == INTERVAL);
            return BinaryExecFunction<interval_t, interval_t, uint8_t, FUNC>;
        }
        default:
            throw RuntimeException("Invalid input data types(" +
                                   Types::dataTypeToString(leftTypeID) + "," +
                                   Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
        }
    }

    template<typename FUNC>
    static scalar_select_func getSelectFunc(DataTypeID leftTypeID, DataTypeID rightTypeID) {
        switch (leftTypeID) {
        case INT64: {
            switch (rightTypeID) {
            case INT64: {
                return BinarySelectFunction<int64_t, int64_t, FUNC>;
            }
            case DOUBLE: {
                return BinarySelectFunction<int64_t, double_t, FUNC>;
            }
            default:
                throw RuntimeException(
                    "Invalid input data types(" + Types::dataTypeToString(leftTypeID) + "," +
                    Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
            }
        }
        case DOUBLE: {
            switch (rightTypeID) {
            case INT64: {
                return BinarySelectFunction<double_t, int64_t, FUNC>;
            }
            case DOUBLE: {
                return BinarySelectFunction<double_t, double_t, FUNC>;
            }
            default:
                throw RuntimeException(
                    "Invalid input data types(" + Types::dataTypeToString(leftTypeID) + "," +
                    Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
            }
        }
        case BOOL: {
            assert(rightTypeID == BOOL);
            return BinarySelectFunction<uint8_t, uint8_t, FUNC>;
        }
        case STRING: {
            assert(rightTypeID == STRING);
            return BinarySelectFunction<ku_string_t, ku_string_t, FUNC>;
        }
        case NODE_ID: {
            assert(rightTypeID == NODE_ID);
            return BinarySelectFunction<nodeID_t, nodeID_t, FUNC>;
        }
        case DATE: {
            switch (rightTypeID) {
            case DATE: {
                return BinarySelectFunction<date_t, date_t, FUNC>;
            }
            case TIMESTAMP: {
                return BinarySelectFunction<date_t, timestamp_t, FUNC>;
            }
            default:
                throw RuntimeException(
                    "Invalid input data types(" + Types::dataTypeToString(leftTypeID) + "," +
                    Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
            }
        }
        case TIMESTAMP: {
            switch (rightTypeID) {
            case DATE: {
                return BinarySelectFunction<timestamp_t, date_t, FUNC>;
            }
            case TIMESTAMP: {
                return BinarySelectFunction<timestamp_t, timestamp_t, FUNC>;
            }
            default:
                throw RuntimeException(
                    "Invalid input data types(" + Types::dataTypeToString(leftTypeID) + "," +
                    Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
            }
        }
        case INTERVAL: {
            assert(rightTypeID == INTERVAL);
            return BinarySelectFunction<interval_t, interval_t, FUNC>;
        }
        default:
            throw RuntimeException("Invalid input data types(" +
                                   Types::dataTypeToString(leftTypeID) + "," +
                                   Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
        }
    }
};

struct EqualsVectorOperation : public VectorComparisonOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::Equals>(EQUALS_FUNC_NAME);
    }
};

struct NotEqualsVectorOperation : public VectorComparisonOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::NotEquals>(
            NOT_EQUALS_FUNC_NAME);
    }
};

struct GreaterThanVectorOperation : public VectorComparisonOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::GreaterThan>(
            GREATER_THAN_FUNC_NAME);
    }
};

struct GreaterThanEqualsVectorOperation : public VectorComparisonOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::GreaterThanEquals>(
            GREATER_THAN_EQUALS_FUNC_NAME);
    }
};

struct LessThanVectorOperation : public VectorComparisonOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::LessThan>(LESS_THAN_FUNC_NAME);
    }
};

struct LessThanEqualsVectorOperation : public VectorComparisonOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::LessThanEquals>(
            LESS_THAN_EQUALS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
