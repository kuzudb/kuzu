#include "include/vector_comparison_operations.h"

namespace graphflow {
namespace function {

scalar_exec_func VectorComparisonOperations::bindExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(isExpressionBinary(expressionType));
    return bindBinaryExecFunction(expressionType, children);
}

scalar_select_func VectorComparisonOperations::bindSelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(isExpressionBinary(expressionType));
    return bindBinarySelectFunction(expressionType, children);
}

scalar_exec_func VectorComparisonOperations::bindBinaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validate(expressionType, leftType.typeID, rightType.typeID);
    switch (leftType.typeID) {
    case BOOL: {
        assert(rightType.typeID == BOOL);
        return bindBinaryExecFunction<uint8_t, uint8_t>(expressionType);
    }
    case INT64: {
        switch (rightType.typeID) {
        case INT64: {
            return bindBinaryExecFunction<int64_t, int64_t>(expressionType);
        }
        case DOUBLE: {
            return bindBinaryExecFunction<int64_t, double_t>(expressionType);
        }
        default:
            assert(false);
        }
    }
    case DOUBLE: {
        switch (rightType.typeID) {
        case INT64: {
            return bindBinaryExecFunction<double_t, int64_t>(expressionType);
        }
        case DOUBLE: {
            return bindBinaryExecFunction<double_t, double_t>(expressionType);
        }
        default:
            assert(false);
        }
    }
    case STRING: {
        assert(rightType.typeID == STRING);
        return bindBinaryExecFunction<gf_string_t, gf_string_t>(expressionType);
    }
    case NODE_ID: {
        assert(rightType.typeID == NODE_ID);
        return bindBinaryExecFunction<nodeID_t, nodeID_t>(expressionType);
    }
    case UNSTRUCTURED: {
        assert(rightType.typeID == UNSTRUCTURED);
        return bindBinaryExecFunction<Value, Value>(expressionType);
    }
    case DATE: {
        assert(rightType.typeID == DATE);
        return bindBinaryExecFunction<date_t, date_t>(expressionType);
    }
    case TIMESTAMP: {
        assert(rightType.typeID == TIMESTAMP);
        return bindBinaryExecFunction<timestamp_t, timestamp_t>(expressionType);
    }
    case INTERVAL: {
        assert(rightType.typeID == INTERVAL);
        return bindBinaryExecFunction<interval_t, interval_t>(expressionType);
    }
    default:
        assert(false);
    }
}

template<typename LEFT_TYPE, typename RIGHT_TYPE>
scalar_exec_func VectorComparisonOperations::bindBinaryExecFunction(ExpressionType expressionType) {
    switch (expressionType) {
    case EQUALS: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, uint8_t, operation::Equals>;
    }
    case NOT_EQUALS: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, uint8_t, operation::NotEquals>;
    }
    case GREATER_THAN: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, uint8_t, operation::GreaterThan>;
    }
    case GREATER_THAN_EQUALS: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, uint8_t, operation::GreaterThanEquals>;
    }
    case LESS_THAN: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, uint8_t, operation::LessThan>;
    }
    case LESS_THAN_EQUALS: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, uint8_t, operation::LessThanEquals>;
    }
    default:
        assert(false);
    }
}

scalar_select_func VectorComparisonOperations::bindBinarySelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validate(expressionType, leftType.typeID, rightType.typeID);
    switch (leftType.typeID) {
    case BOOL: {
        assert(rightType.typeID == BOOL);
        return bindBinarySelectFunction<uint8_t, uint8_t>(expressionType);
    }
    case INT64: {
        switch (rightType.typeID) {
        case INT64: {
            return bindBinarySelectFunction<int64_t, int64_t>(expressionType);
        }
        case DOUBLE: {
            return bindBinarySelectFunction<int64_t, double_t>(expressionType);
        }
        default:
            assert(false);
        }
    }
    case DOUBLE: {
        switch (rightType.typeID) {
        case INT64: {
            return bindBinarySelectFunction<double_t, int64_t>(expressionType);
        }
        case DOUBLE: {
            return bindBinarySelectFunction<double_t, double_t>(expressionType);
        }
        default:
            assert(false);
        }
    }
    case STRING: {
        assert(rightType.typeID == STRING);
        return bindBinarySelectFunction<gf_string_t, gf_string_t>(expressionType);
    }
    case NODE_ID: {
        assert(rightType.typeID == NODE_ID);
        return bindBinarySelectFunction<nodeID_t, nodeID_t>(expressionType);
    }
    case UNSTRUCTURED: {
        assert(rightType.typeID == UNSTRUCTURED);
        return bindBinarySelectFunction<Value, Value>(expressionType);
    }
    case DATE: {
        assert(rightType.typeID == DATE);
        return bindBinarySelectFunction<date_t, date_t>(expressionType);
    }
    case TIMESTAMP: {
        assert(rightType.typeID == TIMESTAMP);
        return bindBinarySelectFunction<timestamp_t, timestamp_t>(expressionType);
    }
    case INTERVAL: {
        assert(rightType.typeID == INTERVAL);
        return bindBinarySelectFunction<interval_t, interval_t>(expressionType);
    }
    default:
        assert(false);
    }
}

template<typename LEFT_TYPE, typename RIGHT_TYPE>
scalar_select_func VectorComparisonOperations::bindBinarySelectFunction(
    ExpressionType expressionType) {
    switch (expressionType) {
    case EQUALS: {
        return BinarySelectFunction<LEFT_TYPE, RIGHT_TYPE, operation::Equals>;
    }
    case NOT_EQUALS: {
        return BinarySelectFunction<LEFT_TYPE, RIGHT_TYPE, operation::NotEquals>;
    }
    case GREATER_THAN: {
        return BinarySelectFunction<LEFT_TYPE, RIGHT_TYPE, operation::GreaterThan>;
    }
    case GREATER_THAN_EQUALS: {
        return BinarySelectFunction<LEFT_TYPE, RIGHT_TYPE, operation::GreaterThanEquals>;
    }
    case LESS_THAN: {
        return BinarySelectFunction<LEFT_TYPE, RIGHT_TYPE, operation::LessThan>;
    }
    case LESS_THAN_EQUALS: {
        return BinarySelectFunction<LEFT_TYPE, RIGHT_TYPE, operation::LessThanEquals>;
    }
    default:
        assert(false);
    }
}

void VectorComparisonOperations::validate(
    ExpressionType expressionType, DataTypeID leftTypeID, DataTypeID rightTypeID) {
    auto isLeftNumerical = Types::isNumericalType(leftTypeID);
    auto isRightNumerical = Types::isNumericalType(rightTypeID);
    // validate both sides are numerical
    if ((isLeftNumerical && !isRightNumerical) || (!isLeftNumerical && isRightNumerical)) {
        throw invalid_argument(expressionTypeToString(expressionType) +
                               " is defined on (Numerical, Numerical) but get (" +
                               Types::dataTypeToString(leftTypeID) + ", " +
                               Types::dataTypeToString(rightTypeID) + ").");
    }
    // otherwise validate both sides are the same type
    if ((!isLeftNumerical && !isRightNumerical) && leftTypeID != rightTypeID) {
        throw invalid_argument(expressionTypeToString(expressionType) +
                               " is defined on the same data types but get (" +
                               Types::dataTypeToString(leftTypeID) + ", " +
                               Types::dataTypeToString(rightTypeID) + ").");
    }
}

} // namespace function
} // namespace graphflow
