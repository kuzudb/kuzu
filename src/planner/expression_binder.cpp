#include "src/planner/include/expression_binder.h"

#include "src/expression/include/logical/logical_rel_expression.h"

namespace graphflow {
namespace planner {

static void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression);
static void validateExpectedType(const LogicalExpression& logicalExpression, DataType expectedType);
static void validateNumericalType(const LogicalExpression& logicalExpression);

shared_ptr<LogicalExpression> ExpressionBinder::bindExpression(
    const ParsedExpression& parsedExpression) {
    shared_ptr<LogicalExpression> expression;
    auto expressionType = parsedExpression.type;
    if (isExpressionBoolConnection(expressionType)) {
        expression = isExpressionUnary(expressionType) ?
                         bindUnaryBooleanExpression(parsedExpression) :
                         bindBinaryBooleanExpression(parsedExpression);
    } else if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
    } else if (isExpressionArithmetic(expressionType)) {
        expression = isExpressionUnary(expressionType) ?
                         bindUnaryArithmeticExpression(parsedExpression) :
                         bindBinaryArithmeticExpression(parsedExpression);
    } else if (isExpressionStringOperator(expressionType)) {
        expression = bindStringOperatorExpression(parsedExpression);
    } else if (isExpressionNullComparison(expressionType)) {
        expression = bindNullComparisonOperatorExpression(parsedExpression);
    } else if (isExpressionLeafLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    } else if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    } else if (!expression) {
        throw invalid_argument(
            "Bind " + expressionTypeToString(expressionType) + " expression is not implemented.");
    }
    expression->rawExpression = parsedExpression.rawExpression;
    if (!parsedExpression.alias.empty()) {
        expression->alias = parsedExpression.alias;
    }
    return expression;
}

static shared_ptr<LogicalExpression> validateAsBoolAndCastIfNecessary(
    shared_ptr<LogicalExpression> expression) {
    if (expression->dataType != UNSTRUCTURED) {
        validateExpectedType(*expression, BOOL);
    } else {
        assert(expression->dataType == UNSTRUCTURED);
        expression = make_shared<LogicalExpression>(CAST_UNKNOWN_TO_BOOL, BOOL, move(expression));
    }
    return expression;
}

shared_ptr<LogicalExpression> ExpressionBinder::bindBinaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children[0]);
    auto right = bindExpression(*parsedExpression.children[1]);
    return make_shared<LogicalExpression>(parsedExpression.type, BOOL,
        validateAsBoolAndCastIfNecessary(left), validateAsBoolAndCastIfNecessary(right));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindUnaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    return make_shared<LogicalExpression>(NOT, BOOL, validateAsBoolAndCastIfNecessary(child));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedLeft = *parsedExpression.children.at(0);
    auto& parsedRight = *parsedExpression.children.at(1);
    if (parsedLeft.type == LITERAL_NULL || parsedRight.type == LITERAL_NULL) {
        if (parsedExpression.type == EQUALS || parsedExpression.type == NOT_EQUALS) {
            return make_shared<LogicalExpression>(LITERAL_BOOLEAN, BOOL, Value(FALSE));
        } else {
            return make_shared<LogicalExpression>(LITERAL_BOOLEAN, BOOL, Value(NULL_BOOL));
        }
    }
    auto left = bindExpression(parsedLeft);
    auto right = bindExpression(parsedRight);
    if (left->dataType == UNSTRUCTURED && right->dataType != UNSTRUCTURED) {
        right = make_shared<LogicalExpression>(CAST_TO_UNKNOWN, UNSTRUCTURED, move(right));
    } else if (left->dataType != UNSTRUCTURED && right->dataType == UNSTRUCTURED) {
        left = make_shared<LogicalExpression>(CAST_TO_UNKNOWN, UNSTRUCTURED, move(left));
    }
    if (isNumericalType(left->dataType)) {
        if (!isNumericalType(right->dataType)) {
            return make_shared<LogicalExpression>(LITERAL_BOOLEAN, BOOL, Value(FALSE));
        }
    } else if (left->dataType != right->dataType) {
        return make_shared<LogicalExpression>(LITERAL_BOOLEAN, BOOL, Value(FALSE));
    }
    return make_shared<LogicalExpression>(parsedExpression.type, BOOL, move(left), move(right));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateNumericalType(*left);
    validateNumericalType(*right);
    DataType resultType;
    if (left->dataType == DOUBLE || right->dataType == DOUBLE) {
        resultType = DOUBLE;
    } else {
        resultType = INT32;
    }
    if (!isExpressionArithmetic(parsedExpression.type)) {
        throw invalid_argument("Should never happen. Cannot bind expression type of " +
                               expressionTypeToString(parsedExpression.type) +
                               " as binary arithmetic expression.");
    }
    return make_shared<LogicalExpression>(
        parsedExpression.type, resultType, move(left), move(right));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindUnaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateNumericalType(*child);
    return make_shared<LogicalExpression>(parsedExpression.type, child->dataType, move(child));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*left, STRING);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateExpectedType(*right, STRING);
    return make_shared<LogicalExpression>(parsedExpression.type, BOOL, move(left), move(right));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindNullComparisonOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    return make_shared<LogicalExpression>(parsedExpression.type, BOOL, move(childExpression));
}

// Note ParsedPropertyExpression is unary ,but LogicalPropertyExpression is leaf.
// Bind property to node or rel. This should change for unstructured properties.
shared_ptr<LogicalExpression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto propertyName = parsedExpression.text;
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    if (NODE == childExpression->dataType) {
        auto node = static_pointer_cast<LogicalNodeExpression>(childExpression);
        if (!catalog.containNodeProperty(node->label, propertyName)) {
            throw invalid_argument("Node " + node->getAliasElseRawExpression() +
                                   " does not have property " + propertyName + ".");
        }
        return make_shared<LogicalExpression>(PROPERTY,
            catalog.getNodePropertyTypeFromString(node->label, propertyName),
            node->name + "." + propertyName);
    }
    if (REL == childExpression->dataType) {
        auto rel = static_pointer_cast<LogicalRelExpression>(childExpression);
        if (!catalog.containRelProperty(rel->label, propertyName)) {
            throw invalid_argument("Rel " + rel->getAliasElseRawExpression() +
                                   " does not have property " + propertyName + ".");
        }
        return make_shared<LogicalExpression>(PROPERTY,
            catalog.getRelPropertyTypeFromString(rel->label, propertyName),
            rel->name + "." + propertyName);
    }
    throw invalid_argument("Type mismatch: expect NODE or REL, but " +
                           childExpression->rawExpression + " was " +
                           dataTypeToString(childExpression->dataType) + ".");
}

// COUNT(*) is the only function expression supported
shared_ptr<LogicalExpression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    assert("COUNT_STAR" == parsedExpression.text);
    return make_shared<LogicalExpression>(FUNCTION, INT64, COUNT_STAR);
}

shared_ptr<LogicalExpression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto literalVal = parsedExpression.text;
    auto literalType = parsedExpression.type;
    switch (literalType) {
    case LITERAL_INT:
        return make_shared<LogicalExpression>(LITERAL_INT, INT32, Value(stoi(literalVal)));
    case LITERAL_DOUBLE:
        return make_shared<LogicalExpression>(LITERAL_DOUBLE, DOUBLE, Value(stod(literalVal)));
    case LITERAL_BOOLEAN:
        return make_shared<LogicalExpression>(
            LITERAL_BOOLEAN, BOOL, Value((uint8_t)("true" == literalVal)));
    case LITERAL_STRING:
        return make_shared<LogicalExpression>(
            LITERAL_STRING, STRING, Value(literalVal.substr(1, literalVal.size() - 2)));
    default:
        throw invalid_argument("Literal " + parsedExpression.rawExpression + "is not defined.");
    }
}

shared_ptr<LogicalExpression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto variableName = parsedExpression.text;
    if (variablesInScope.contains(variableName)) {
        return variablesInScope.at(variableName);
    }
    throw invalid_argument("Variable " + parsedExpression.rawExpression + " not defined.");
}

void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression) {
    for (auto& child : parsedExpression.children) {
        if (LITERAL_NULL == child->type) {
            throw invalid_argument(
                "Expression " + child->rawExpression + " cannot have null literal children.");
        }
    }
}

void validateExpectedType(const LogicalExpression& logicalExpression, DataType expectedType) {
    auto dataType = logicalExpression.dataType;
    if (expectedType != dataType) {
        throw invalid_argument("Expression " + logicalExpression.rawExpression + " has data type " +
                               dataTypeToString(dataType) + " expect " +
                               dataTypeToString(expectedType) + ".");
    }
}

void validateNumericalType(const LogicalExpression& logicalExpression) {
    auto dataType = logicalExpression.dataType;
    if (!isNumericalType(dataType)) {
        throw invalid_argument("Expression " + logicalExpression.rawExpression + " has data type " +
                               dataTypeToString(dataType) + " expect numerical type.");
    }
}

} // namespace planner
} // namespace graphflow
