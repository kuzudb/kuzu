#include "function/boolean/vector_boolean_operations.h"

#include "function/boolean/boolean_operations.h"

namespace kuzu {
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
    assert(leftType.typeID == BOOL && rightType.typeID == BOOL);
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
    assert(leftType.typeID == BOOL && rightType.typeID == BOOL);
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
    assert(children.size() == 1 && children[0]->dataType.typeID == BOOL);
    switch (expressionType) {
    case NOT: {
        return UnaryBooleanExecFunction<operation::Not>;
    }
    default:
        assert(false);
    }
}

scalar_select_func VectorBooleanOperations::bindUnarySelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 1 && children[0]->dataType.typeID == BOOL);
    switch (expressionType) {
    case NOT: {
        return UnaryBooleanSelectFunction<operation::Not>;
    }
    default:
        assert(false);
    }
}

} // namespace function
} // namespace kuzu
