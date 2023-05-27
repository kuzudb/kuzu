#include "function/boolean/vector_boolean_operations.h"

#include "function/boolean/boolean_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void VectorBooleanOperations::bindExecFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_exec_func& func) {
    if (isExpressionBinary(expressionType)) {
        bindBinaryExecFunction(expressionType, children, func);
    } else {
        assert(isExpressionUnary(expressionType));
        bindUnaryExecFunction(expressionType, children, func);
    }
}

void VectorBooleanOperations::bindSelectFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_select_func& func) {
    if (isExpressionBinary(expressionType)) {
        bindBinarySelectFunction(expressionType, children, func);
    } else {
        assert(isExpressionUnary(expressionType));
        bindUnarySelectFunction(expressionType, children, func);
    }
}

void VectorBooleanOperations::bindBinaryExecFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_exec_func& func) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    assert(leftType.getLogicalTypeID() == LogicalTypeID::BOOL &&
           rightType.getLogicalTypeID() == LogicalTypeID::BOOL);
    switch (expressionType) {
    case AND: {
        func = &BinaryBooleanExecFunction<operation::And>;
        return;
    }
    case OR: {
        func = &BinaryBooleanExecFunction<operation::Or>;
        return;
    }
    case XOR: {
        func = &BinaryBooleanExecFunction<operation::Xor>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               " for VectorBooleanOperations::bindBinaryExecFunction.");
    }
}

void VectorBooleanOperations::bindBinarySelectFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_select_func& func) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    assert(leftType.getLogicalTypeID() == LogicalTypeID::BOOL &&
           rightType.getLogicalTypeID() == LogicalTypeID::BOOL);
    switch (expressionType) {
    case AND: {
        func = &BinaryBooleanSelectFunction<operation::And>;
        return;
    }
    case OR: {
        func = &BinaryBooleanSelectFunction<operation::Or>;
        return;
    }
    case XOR: {
        func = &BinaryBooleanSelectFunction<operation::Xor>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               " for VectorBooleanOperations::bindBinarySelectFunction.");
    }
}

void VectorBooleanOperations::bindUnaryExecFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_exec_func& func) {
    assert(children.size() == 1 && children[0]->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    switch (expressionType) {
    case NOT: {
        func = &UnaryBooleanExecFunction<operation::Not>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               " for VectorBooleanOperations::bindUnaryExecFunction.");
    }
}

void VectorBooleanOperations::bindUnarySelectFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_select_func& func) {
    assert(children.size() == 1 && children[0]->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    switch (expressionType) {
    case NOT: {
        func = &UnaryBooleanSelectFunction<operation::Not>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               " for VectorBooleanOperations::bindUnaryExecFunction.");
    }
}

} // namespace function
} // namespace kuzu
