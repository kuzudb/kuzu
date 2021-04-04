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
    auto dataType = logicalExpression.getDataType();
    if (expectedType != dataType) {
        throw invalid_argument("Expression: " + logicalExpression.getRawExpression() +
                               " has data type: " + dataTypeToString(dataType) +
                               " expect: " + dataTypeToString(expectedType));
    }
}

void validateNumericalType(const LogicalExpression& logicalExpression) {
    auto dataType = logicalExpression.getDataType();
    if (!isNumericalType(dataType)) {
        throw invalid_argument("Expression: " + logicalExpression.getRawExpression() +
                               " has data type: " + dataTypeToString(dataType) +
                               " expect numerical type.");
    }
}

unique_ptr<LogicalExpression> ExpressionBinder::bindExpression(
    const ParsedExpression& parsedExpression) {
    auto expressionType = parsedExpression.type;
    if (isExpressionBoolConnection(expressionType)) {
        return isExpressionUnary(expressionType) ?
                   bindUnaryBoolConnectionExpression(parsedExpression) :
                   bindBinaryBoolConnectionExpression(parsedExpression);
    }
    if (isExpressionComparison(expressionType)) {
        return bindComparisonExpression(parsedExpression);
    }
    if (isExpressionArithmetic(expressionType)) {
        return isExpressionUnary(expressionType) ? bindUnaryArithmeticExpression(parsedExpression) :
                                                   bindBinaryArithmeticExpression(parsedExpression);
    }
    if (isExpressionStringOperator(expressionType)) {
        return bindStringOperatorExpression(parsedExpression);
    }
    if (isExpressionNullComparison(expressionType)) {
        return bindNullComparisonOperatorExpression(parsedExpression);
    }
    if (isExpressionLeafLiteral(expressionType)) {
        return bindLiteralExpression(parsedExpression);
    }
    if (FUNCTION == expressionType) {
        throw invalid_argument("Bind expression with type FUNCTION is not yet implemented.");
    }
    if (PROPERTY == expressionType) {
        return bindPropertyExpression(parsedExpression);
    }
    if (VARIABLE == expressionType) {
        return bindVariableExpression(parsedExpression);
    }
    throw invalid_argument("Bind expression with type: " + expressionTypeToString(expressionType) +
                           " should never happen.");
}

unique_ptr<LogicalExpression> ExpressionBinder::bindBinaryBoolConnectionExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*left, BOOL);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateExpectedType(*right, BOOL);
    return make_unique<LogicalExpression>(
        parsedExpression.type, BOOL, move(left), move(right), parsedExpression.rawExpression);
}

unique_ptr<LogicalExpression> ExpressionBinder::bindUnaryBoolConnectionExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*child, BOOL);
    return make_unique<LogicalExpression>(NOT, BOOL, move(child), parsedExpression.rawExpression);
}

unique_ptr<LogicalExpression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedLeft = *parsedExpression.children.at(0);
    auto& parsedRight = *parsedExpression.children.at(1);
    if (LITERAL_NULL == parsedLeft.type || LITERAL_NULL == parsedRight.type) {
        // rewrite == null as IS NULL
        if (EQUALS == parsedExpression.type) {
            return make_unique<LogicalExpression>(IS_NULL, BOOL,
                LITERAL_NULL == parsedLeft.type ? bindExpression(parsedRight) :
                                                  bindExpression(parsedLeft));
        }
        // rewrite <> null as IS NOT NULL
        if (NOT_EQUALS == parsedExpression.type) {
            return make_unique<LogicalExpression>(IS_NOT_NULL, BOOL,
                LITERAL_NULL == parsedLeft.type ? bindExpression(parsedRight) :
                                                  bindExpression(parsedLeft));
        }
    }
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(parsedLeft);
    auto right = bindExpression(parsedRight);
    isNumericalType(left->getDataType()) ? validateNumericalType(*right) :
                                           validateExpectedType(*right, left->getDataType());
    return make_unique<LogicalExpression>(
        parsedExpression.type, BOOL, move(left), move(right), parsedExpression.rawExpression);
}

unique_ptr<LogicalExpression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateNumericalType(*left);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateNumericalType(*right);
    DataType resultType;
    if (DOUBLE == left->getDataType() || DOUBLE == right->getDataType()) {
        resultType = DOUBLE;
    } else if (INT64 == left->getDataType() || INT64 == right->getDataType()) {
        resultType = INT64;
    } else {
        resultType = INT32;
    }
    auto expressionType = parsedExpression.type;
    switch (expressionType) {
    case ADD:
    case SUBTRACT:
    case MULTIPLY:
    case POWER:
        return make_unique<LogicalExpression>(
            expressionType, resultType, move(left), move(right), parsedExpression.rawExpression);
    case DIVIDE:
        return make_unique<LogicalExpression>(
            DIVIDE, DOUBLE, move(left), move(right), parsedExpression.rawExpression);
    case MODULO:
        return make_unique<LogicalExpression>(
            MODULO, INT32, move(left), move(right), parsedExpression.rawExpression);
    default:
        throw invalid_argument("Should never happen. Cannot bind expression type of " +
                               expressionTypeToString(expressionType) +
                               " as binary arithmetic expression.");
    }
}

unique_ptr<LogicalExpression> ExpressionBinder::bindUnaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateNumericalType(*child);
    return make_unique<LogicalExpression>(
        parsedExpression.type, child->getDataType(), move(child), parsedExpression.rawExpression);
}

unique_ptr<LogicalExpression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*left, STRING);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateExpectedType(*right, STRING);
    return make_unique<LogicalExpression>(
        parsedExpression.type, BOOL, move(left), move(right), parsedExpression.rawExpression);
}

unique_ptr<LogicalExpression> ExpressionBinder::bindNullComparisonOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    return make_unique<LogicalExpression>(
        parsedExpression.type, BOOL, move(childExpression), parsedExpression.rawExpression);
}

// Note ParsedPropertyExpression is unary ,but LogicalPropertyExpression is leaf.
// Bind property to node or rel. This should change for unstructured properties.
unique_ptr<LogicalExpression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto propertyName = parsedExpression.text;
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    if (NODE == childExpression->getDataType()) {
        auto nodeName = childExpression->getVariableName();
        auto nodeLabel = graphInScope.getQueryNode(nodeName)->getLabel();
        if (!catalog.containNodeProperty(nodeLabel, propertyName)) {
            throw invalid_argument(
                "Node: " + nodeName + " does not have property: " + propertyName);
        }
        return make_unique<LogicalExpression>(PROPERTY,
            catalog.getNodePropertyTypeFromString(nodeLabel, propertyName),
            nodeName + "." + propertyName, parsedExpression.rawExpression);
    }
    if (REL == childExpression->getDataType()) {
        auto relName = childExpression->getVariableName();
        auto relLabel = graphInScope.getQueryRel(relName)->getLabel();
        if (!catalog.containRelProperty(relLabel, propertyName)) {
            throw invalid_argument("Rel: " + relName + " does not have property: " + propertyName);
        }
        return make_unique<LogicalExpression>(PROPERTY,
            catalog.getRelPropertyTypeFromString(relLabel, propertyName),
            relName + "." + propertyName, parsedExpression.rawExpression);
    }
    throw invalid_argument(
        "Property: " + parsedExpression.rawExpression + " is not associated with any node or rel.");
}

unique_ptr<LogicalExpression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto literalVal = parsedExpression.text;
    auto literalType = parsedExpression.type;
    switch (literalType) {
    case LITERAL_INT:
        return make_unique<LogicalExpression>(
            LITERAL_INT, INT32, Literal(stoi(literalVal)), parsedExpression.rawExpression);
    case LITERAL_DOUBLE:
        return make_unique<LogicalExpression>(
            LITERAL_DOUBLE, DOUBLE, Literal(stod(literalVal)), parsedExpression.rawExpression);
    case LITERAL_BOOLEAN:
        return make_unique<LogicalExpression>(LITERAL_BOOLEAN, BOOL,
            Literal((uint8_t)("true" == literalVal)), parsedExpression.rawExpression);
    case LITERAL_STRING:
        return make_unique<LogicalExpression>(LITERAL_STRING, STRING,
            Literal(literalVal.substr(1, literalVal.size() - 2)), parsedExpression.rawExpression);
    default:
        throw invalid_argument("Literal: " + parsedExpression.rawExpression + "is not defined.");
    }
}

// Bind variable to either node or rel in the query graph
// Should change once we have WITH statement
unique_ptr<LogicalExpression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto varName = parsedExpression.text;
    if (graphInScope.containsQueryNode(varName)) {
        return make_unique<LogicalExpression>(
            VARIABLE, NODE, varName, parsedExpression.rawExpression);
    }
    if (graphInScope.containsQueryRel(varName)) {
        return make_unique<LogicalExpression>(
            VARIABLE, REL, varName, parsedExpression.rawExpression);
    }
    throw invalid_argument("Variable: " + parsedExpression.rawExpression + " is not in scope.");
}

} // namespace planner
} // namespace graphflow
