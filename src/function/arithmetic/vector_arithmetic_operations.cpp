#include "include/vector_arithmetic_operations.h"

#include "operations/include/arithmetic_operations.h"

#include "src/function/string/include/vector_string_operations.h"

namespace graphflow {
namespace function {

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    if (isExpressionBinary(expressionType)) {
        return bindBinaryExecFunction(expressionType, children);
    } else if (isExpressionUnary(expressionType)) {
        return bindUnaryExecFunction(expressionType, children);
    }
    assert(false);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindAbsExecFunction(
    const expression_vector& children) {
    validateNumParameters(ABS_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Abs>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindFloorExecFunction(
    const expression_vector& children) {
    validateNumParameters(FLOOR_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Floor>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindCeilExecFunction(
    const expression_vector& children) {
    validateNumParameters(CEIL_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Ceil>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindBinaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    switch (children[0]->dataType) {
    case STRING: {
        return bindStringArithmeticExecFunction(expressionType, children);
    }
    case DATE: {
        return bindDateArithmeticExecFunction(expressionType, children);
    }
    case TIMESTAMP: {
        return bindTimestampArithmeticExecFunction(expressionType, children);
    }
    case INTERVAL: {
        return bindIntervalArithmeticExecFunction(expressionType, children);
    }
    default: {
        return bindNumericalArithmeticExecFunction(expressionType, children);
    }
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindStringArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(expressionType == ADD);
    validateParameterType(expressionTypeToString(expressionType), *children[1], STRING);
    return make_pair(VectorStringOperations::bindExecFunction(expressionType, children), STRING);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindDateArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    switch (expressionType) {
    case ADD: {
        validateParameterType(expressionTypeToString(expressionType), *children[1],
            unordered_set<DataType>{INT64, INTERVAL});
        switch (children[1]->dataType) {
        case INT64: { // date + int → date
            return make_pair(BinaryExecFunction<date_t, int64_t, date_t, operation::Add>, DATE);
        }
        case INTERVAL: { // date + interval → date
            return make_pair(BinaryExecFunction<date_t, interval_t, date_t, operation::Add>, DATE);
        }
        default:
            assert(false);
        }
    }
    case SUBTRACT: {
        validateParameterType(expressionTypeToString(expressionType), *children[1],
            unordered_set<DataType>{DATE, INT64, INTERVAL});
        switch (children[1]->dataType) {
        case DATE: { // date - date → integer
            return make_pair(
                BinaryExecFunction<date_t, date_t, int64_t, operation::Subtract>, INT64);
        }
        case INT64: { // date - integer → date
            return make_pair(
                BinaryExecFunction<date_t, int64_t, date_t, operation::Subtract>, DATE);
        }
        case INTERVAL: { // date - interval → date
            return make_pair(
                BinaryExecFunction<date_t, interval_t, date_t, operation::Subtract>, DATE);
        }
        default:
            assert(false);
        }
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindTimestampArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    switch (expressionType) {
    case ADD: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INTERVAL);
        switch (children[1]->dataType) {
        case INTERVAL: { // timestamp + interval → timestamp
            return make_pair(
                BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Add>,
                TIMESTAMP);
        }
        default:
            assert(false);
        }
    }
    case SUBTRACT: {
        validateParameterType(expressionTypeToString(expressionType), *children[1],
            unordered_set<DataType>{TIMESTAMP, INTERVAL});
        switch (children[1]->dataType) {
        case TIMESTAMP: { // timestamp - timestamp → interval
            return make_pair(
                BinaryExecFunction<timestamp_t, timestamp_t, interval_t, operation::Subtract>,
                INTERVAL);
        }
        case INTERVAL: { // timestamp - interval → timestamp
            return make_pair(
                BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Subtract>,
                TIMESTAMP);
        }
        default:
            assert(false);
        }
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindIntervalArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    switch (expressionType) {
    case ADD: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INTERVAL);
        switch (children[1]->dataType) {
        case INTERVAL: { // interval +interval → interval
            return make_pair(
                BinaryExecFunction<interval_t, interval_t, interval_t, operation::Add>, INTERVAL);
        }
        default:
            assert(false);
        }
    }
    case SUBTRACT: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INTERVAL);
        switch (children[1]->dataType) {
        case INTERVAL: { // interval - interval → interval
            return make_pair(
                BinaryExecFunction<interval_t, interval_t, interval_t, operation::Subtract>,
                INTERVAL);
        }
        default:
            assert(false);
        }
    }
    case DIVIDE: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INT64);
        switch (children[1]->dataType) {
        case INT64: { // interval / int → interval
            return make_pair(
                BinaryExecFunction<interval_t, int64_t, interval_t, operation::Divide>, INTERVAL);
        }
        default:
            assert(false);
        }
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindNumericalArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validateParameterType(
        expressionTypeToString(expressionType), *children[0], {INT64, DOUBLE, UNSTRUCTURED});
    validateParameterType(
        expressionTypeToString(expressionType), *children[1], {INT64, DOUBLE, UNSTRUCTURED});
    switch (leftType) {
    case INT64: {
        switch (rightType) {
        case INT64: {
            return make_pair(
                bindNumericalArithmeticExecFunction<int64_t, int64_t, int64_t>(expressionType),
                INT64);
        }
        case DOUBLE: {
            return make_pair(
                bindNumericalArithmeticExecFunction<int64_t, double_t, double_t>(expressionType),
                DOUBLE);
        }
        default:
            assert(false);
        }
    }
    case DOUBLE: {
        switch (rightType) {
        case INT64: {
            return make_pair(
                bindNumericalArithmeticExecFunction<double_t, int64_t, double_t>(expressionType),
                DOUBLE);
        }
        case DOUBLE: {
            return make_pair(
                bindNumericalArithmeticExecFunction<double_t, double_t, double_t>(expressionType),
                DOUBLE);
        }
        default:
            assert(false);
        }
    }
    case UNSTRUCTURED: {
        assert(rightType == UNSTRUCTURED);
        return make_pair(
            bindNumericalArithmeticExecFunction<Value, Value, Value>(expressionType), UNSTRUCTURED);
    }
    default:
        assert(false);
    }
}

template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE>
scalar_exec_func VectorArithmeticOperations::bindNumericalArithmeticExecFunction(
    ExpressionType expressionType) {
    switch (expressionType) {
    case ADD: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Add>;
    }
    case SUBTRACT: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Subtract>;
    }
    case MULTIPLY: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Multiply>;
    }
    case DIVIDE: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Divide>;
    }
    case MODULO: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Modulo>;
    }
    case POWER: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Power>;
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindUnaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 1);
    assert(expressionType == NEGATE);
    return bindUnaryExecFunction<operation::Negate>(children[0]->dataType);
}

template<typename FUNC>
pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindUnaryExecFunction(
    DataType operandType) {
    switch (operandType) {
    case INT64: {
        return make_pair(UnaryExecFunction<int64_t, int64_t, FUNC>, INT64);
    }
    case DOUBLE: {
        return make_pair(UnaryExecFunction<double_t, double_t, FUNC>, DOUBLE);
    }
    case UNSTRUCTURED: {
        return make_pair(UnaryExecFunction<Value, Value, FUNC>, UNSTRUCTURED);
    }
    default:
        assert(false);
    }
}

} // namespace function
} // namespace graphflow
