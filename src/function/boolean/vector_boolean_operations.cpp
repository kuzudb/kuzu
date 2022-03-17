#include "include/vector_boolean_operations.h"

#include "operations/include/boolean_operations.h"

namespace graphflow {
namespace function {

scalar_exec_func VectorBooleanOperations::bindExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    if (isExpressionBinary(expressionType)) {
        return bindBinaryExecFunction(expressionType, children);
    } else if (isExpressionUnary(expressionType)) {
        return bindUnaryExecFunction(expressionType, children);
    }
    assert(false);
}

scalar_select_func VectorBooleanOperations::bindSelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    if (isExpressionBinary(expressionType)) {
        return bindBinarySelectFunction(expressionType, children);
    } else if (isExpressionUnary(expressionType)) {
        return bindUnarySelectFunction(expressionType, children);
    }
    assert(false);
}

scalar_exec_func VectorBooleanOperations::bindBinaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validate(expressionType, leftType, rightType);
    switch (expressionType) {
    case AND: {
        return BinaryBooleanExecFunction<operation::And>;
    }
    case OR: {
        return BinaryBooleanExecFunction<operation::Or>;
    }
    case XOR: {
        return BinaryBooleanExecFunction<operation::Xor>;
    }
    default:
        assert(false);
    }
}

scalar_select_func VectorBooleanOperations::bindBinarySelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validate(expressionType, leftType, rightType);
    switch (expressionType) {
    case AND: {
        return BinaryBooleanSelectFunction<operation::And>;
    }
    case OR: {
        return BinaryBooleanSelectFunction<operation::Or>;
    }
    case XOR: {
        return BinaryBooleanSelectFunction<operation::Xor>;
    }
    default:
        assert(false);
    }
}

scalar_exec_func VectorBooleanOperations::bindUnaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 1);
    auto childType = children[0]->dataType;
    validate(expressionType, childType);
    switch (expressionType) {
    case NOT: {
        return UnaryExecFunction<bool, uint8_t, operation::Not>;
    }
    default:
        assert(false);
    }
}

scalar_select_func VectorBooleanOperations::bindUnarySelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 1);
    auto childType = children[0]->dataType;
    validate(expressionType, childType);
    switch (expressionType) {
    case NOT: {
        return UnarySelectFunction<bool, operation::Not>;
    }
    default:
        assert(false);
    }
}

void VectorBooleanOperations::validate(
    ExpressionType expressionType, DataType leftType, DataType rightType) {
    if (leftType != BOOL || rightType != BOOL) {
        throw invalid_argument(expressionTypeToString(expressionType) +
                               " is defined on (BOOL, BOOL) but get (" +
                               TypeUtils::dataTypeToString(leftType) + ", " +
                               TypeUtils::dataTypeToString(rightType) + ").");
    }
}

void VectorBooleanOperations::validate(ExpressionType expressionType, DataType childType) {
    if (childType != BOOL) {
        throw invalid_argument(expressionTypeToString(expressionType) +
                               " is defined on (BOOL) but get (" +
                               TypeUtils::dataTypeToString(childType) + ").");
    }
}

} // namespace function
} // namespace graphflow
