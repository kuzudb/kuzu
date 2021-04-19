#include "src/planner/include/expression_binder.h"

namespace graphflow {
namespace planner {

static void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression);

static void validateExpectedType(const LogicalExpression& logicalExpression, DataType expectedType);

static void validateNumericalType(const LogicalExpression& logicalExpression);

void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression) {
    for (auto& child : parsedExpression.children) {
        if (LITERAL_NULL == child->type) {
            throw invalid_argument(
                "Expression: " + child->rawExpression + " cannot have null literal children");
        }
    }
}

void validateExpectedType(const LogicalExpression& logicalExpression, DataType expectedType) {
    auto dataType = logicalExpression.dataType;
    if (expectedType != dataType) {
        throw invalid_argument("Expression: " + logicalExpression.rawExpression +
                               " has data type: " + dataTypeToString(dataType) +
                               " expect: " + dataTypeToString(expectedType));
    }
}

void validateNumericalType(const LogicalExpression& logicalExpression) {
    auto dataType = logicalExpression.dataType;
    if (!isNumericalType(dataType)) {
        throw invalid_argument("Expression: " + logicalExpression.rawExpression +
                               " has data type: " + dataTypeToString(dataType) +
                               " expect numerical type.");
    }
}

shared_ptr<LogicalExpression> ExpressionBinder::bindExpression(
    const ParsedExpression& parsedExpression) {
    shared_ptr<LogicalExpression> expression;
    auto expressionType = parsedExpression.type;
    if (isExpressionBoolConnection(expressionType)) {
        expression = isExpressionUnary(expressionType) ?
                         bindUnaryBoolConnectionExpression(parsedExpression) :
                         bindBinaryBoolConnectionExpression(parsedExpression);
    }
    if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
    }
    if (isExpressionArithmetic(expressionType)) {
        expression = isExpressionUnary(expressionType) ?
                         bindUnaryArithmeticExpression(parsedExpression) :
                         bindBinaryArithmeticExpression(parsedExpression);
    }
    if (isExpressionStringOperator(expressionType)) {
        expression = bindStringOperatorExpression(parsedExpression);
    }
    if (isExpressionNullComparison(expressionType)) {
        expression = bindNullComparisonOperatorExpression(parsedExpression);
    }
    if (isExpressionLeafLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    }
    if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    }
    if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    }
    if (VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    }
    if (!expression) {
        throw invalid_argument(
            "Bind " + expressionTypeToString(expressionType) + " expression is not implemented.");
    }
    expression->rawExpression = parsedExpression.rawExpression;
    if (!parsedExpression.alias.empty()) {
        expression->alias = parsedExpression.alias;
    }
    return expression;
}

shared_ptr<LogicalExpression> ExpressionBinder::bindBinaryBoolConnectionExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*left, BOOL);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateExpectedType(*right, BOOL);
    return make_shared<LogicalExpression>(parsedExpression.type, BOOL, move(left), move(right));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindUnaryBoolConnectionExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*child, BOOL);
    return make_shared<LogicalExpression>(NOT, BOOL, move(child));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedLeft = *parsedExpression.children.at(0);
    auto& parsedRight = *parsedExpression.children.at(1);
    if (LITERAL_NULL == parsedLeft.type || LITERAL_NULL == parsedRight.type) {
        // rewrite == null as IS NULL
        if (EQUALS == parsedExpression.type) {
            return make_shared<LogicalExpression>(IS_NULL, BOOL,
                LITERAL_NULL == parsedLeft.type ? bindExpression(parsedRight) :
                                                  bindExpression(parsedLeft));
        }
        // rewrite <> null as IS NOT NULL
        if (NOT_EQUALS == parsedExpression.type) {
            return make_shared<LogicalExpression>(IS_NOT_NULL, BOOL,
                LITERAL_NULL == parsedLeft.type ? bindExpression(parsedRight) :
                                                  bindExpression(parsedLeft));
        }
    }
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(parsedLeft);
    auto right = bindExpression(parsedRight);
    isNumericalType(left->dataType) ? validateNumericalType(*right) :
                                      validateExpectedType(*right, left->dataType);
    return make_shared<LogicalExpression>(parsedExpression.type, BOOL, move(left), move(right));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateNumericalType(*left);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateNumericalType(*right);
    DataType resultType;
    if (DOUBLE == left->dataType || DOUBLE == right->dataType) {
        resultType = DOUBLE;
    } else {
        resultType = INT32;
    }
    auto expressionType = parsedExpression.type;
    switch (expressionType) {
    case ADD:
    case SUBTRACT:
    case MULTIPLY:
    case POWER:
    case DIVIDE:
    case MODULO:
        return make_shared<LogicalExpression>(expressionType, resultType, move(left), move(right));
    default:
        throw invalid_argument("Should never happen. Cannot bind expression type of " +
                               expressionTypeToString(expressionType) +
                               " as binary arithmetic expression.");
    }
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
        auto nodeName = childExpression->variableName;
        auto nodeLabel = graphInScope.getQueryNode(nodeName)->label;
        if (!catalog.containNodeProperty(nodeLabel, propertyName)) {
            throw invalid_argument(
                "Node: " + nodeName + " does not have property: " + propertyName);
        }
        return make_shared<LogicalExpression>(PROPERTY,
            catalog.getNodePropertyTypeFromString(nodeLabel, propertyName),
            nodeName + "." + propertyName);
    }
    if (REL == childExpression->dataType) {
        auto relName = childExpression->variableName;
        auto relLabel = graphInScope.getQueryRel(relName)->label;
        if (!catalog.containRelProperty(relLabel, propertyName)) {
            throw invalid_argument("Rel: " + relName + " does not have property: " + propertyName);
        }
        return make_shared<LogicalExpression>(PROPERTY,
            catalog.getRelPropertyTypeFromString(relLabel, propertyName),
            relName + "." + propertyName);
    }
    throw invalid_argument(
        "Property: " + parsedExpression.rawExpression + " is not associated with any node or rel.");
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
        throw invalid_argument("Literal: " + parsedExpression.rawExpression + "is not defined.");
    }
}

// Bind variable to either node or rel in the query graph
// Should change once we have WITH statement
shared_ptr<LogicalExpression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto varName = parsedExpression.text;
    if (graphInScope.containsQueryNode(varName)) {
        return make_shared<LogicalExpression>(VARIABLE, NODE, varName);
    }
    if (graphInScope.containsQueryRel(varName)) {
        return make_shared<LogicalExpression>(VARIABLE, REL, varName);
    }
    throw invalid_argument("Variable: " + parsedExpression.rawExpression + " is not in scope.");
}

} // namespace planner
} // namespace graphflow
