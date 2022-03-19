#include "include/vector_string_operations.h"

#include "operations/include/concat_operation.h"
#include "operations/include/contains_operation.h"
#include "operations/include/starts_with_operation.h"

namespace graphflow {
namespace function {

scalar_exec_func VectorStringOperations::bindExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    return bindBinaryExecFunction(expressionType, children);
}

scalar_select_func VectorStringOperations::bindSelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    return bindBinarySelectFunction(expressionType, children);
}

scalar_exec_func VectorStringOperations::bindBinaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validate(expressionType, leftType, rightType);
    switch (expressionType) {
    case ADD: {
        return BinaryExecFunction<gf_string_t, gf_string_t, gf_string_t, operation::Concat>;
    }
    case CONTAINS: {
        return BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::Contains>;
    }
    case STARTS_WITH: {
        return BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::StartsWith>;
    }
    default:
        assert(false);
    }
}

scalar_select_func VectorStringOperations::bindBinarySelectFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validate(expressionType, leftType, rightType);
    switch (expressionType) {
    case CONTAINS: {
        return BinarySelectFunction<gf_string_t, gf_string_t, operation::Contains>;
    }
    case STARTS_WITH: {
        return BinarySelectFunction<gf_string_t, gf_string_t, operation::StartsWith>;
    }
    default:
        assert(false);
    }
}

void VectorStringOperations::validate(
    ExpressionType expressionType, DataType leftType, DataType rightType) {
    if (leftType != STRING || rightType != STRING) {
        throw invalid_argument(expressionTypeToString(expressionType) +
                               " is defined on (STRING, STRING) but get (" +
                               TypeUtils::dataTypeToString(leftType) + ", " +
                               TypeUtils::dataTypeToString(rightType) + ").");
    }
}

} // namespace function
} // namespace graphflow
