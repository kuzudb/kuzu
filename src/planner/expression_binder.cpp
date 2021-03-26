#include "src/planner/include/expression_binder.h"

namespace graphflow {
namespace planner {

static void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression);

static void validateExpectedType(const LogicalExpression& logicalExpression, DataType expectedType);

static void validateNumericalType(const LogicalExpression& logicalExpression);

void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression) {
    for (auto i = 0ul; i < parsedExpression.getNumChildren(); ++i) {
        if (LITERAL_NULL == parsedExpression.getChildren(i).getType()) {
            throw invalid_argument(
                "Expression of type: " + expressionTypeToString(parsedExpression.getType()) +
                " cannot have null literal children");
        }
    }
}

void validateExpectedType(const LogicalExpression& logicalExpression, DataType expectedType) {
    auto dataType = logicalExpression.getDataType();
    if (expectedType != dataType) {
        throw invalid_argument("LogicalExpression return data type: " + dataTypeToString(dataType) +
                               " expect: " + dataTypeToString(expectedType));
    }
}

void validateNumericalType(const LogicalExpression& logicalExpression) {
    auto dataType = logicalExpression.getDataType();
    if (!isNumericalType(dataType)) {
        throw invalid_argument("LogicalExpression return data type: " + dataTypeToString(dataType) +
                               " expect numerical type.");
    }
}

unique_ptr<LogicalExpression> ExpressionBinder::bindExpression(
    const ParsedExpression& parsedExpression) {
    auto expressionType = parsedExpression.getType();
    switch (expressionType) {
    case OR:
    case XOR:
    case AND:
    case NOT:
        return bindBoolConnectionExpression(parsedExpression);
    case EQUALS:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_EQUALS:
    case LESS_THAN:
    case LESS_THAN_EQUALS:
        return bindComparisonExpression(parsedExpression);
    case ADD:
    case SUBTRACT:
    case MULTIPLY:
    case DIVIDE:
    case MODULO:
    case POWER:
        return bindBinaryArithmeticExpression(parsedExpression);
    case NEGATE:
        return bindUnaryArithmeticExpression(parsedExpression);
    case STARTS_WITH:
    case ENDS_WITH:
    case CONTAINS:
        return bindStringOperatorExpression(parsedExpression);
    case IS_NULL:
    case IS_NOT_NULL:
        return bindNullComparisonOperatorExpression(parsedExpression);
    case PROPERTY:
        return bindPropertyExpression(parsedExpression);
    case FUNCTION:
        throw invalid_argument("Bind expression with type FUNCTION is not yet implemented.");
    case LITERAL_INT:
    case LITERAL_DOUBLE:
    case LITERAL_STRING:
    case LITERAL_BOOLEAN:
    case LITERAL_NULL:
        return bindLiteralExpression(parsedExpression);
    case VARIABLE:
        return bindVariableExpression(parsedExpression);
    default:
        throw invalid_argument("Bind expression with type: " +
                               expressionTypeToString(expressionType) + " should never happen.");
    }
}

unique_ptr<LogicalExpression> ExpressionBinder::bindBoolConnectionExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto expressionType = parsedExpression.getType();
    switch (expressionType) {
    case OR:
    case XOR:
    case AND:
        return buildBinaryASTForBoolConnectionExpression(parsedExpression, expressionType);
    case NOT: {
        auto child = bindExpression(parsedExpression.getChildren(0));
        validateExpectedType(*child, BOOL);
        return make_unique<LogicalExpression>(NOT, BOOL, move(child));
    }
    default:
        throw invalid_argument("Should never happen. Cannot bind expression type of " +
                               expressionTypeToString(expressionType) +
                               " as bool connection expression.");
    }
}

unique_ptr<LogicalExpression> ExpressionBinder::buildBinaryASTForBoolConnectionExpression(
    const ParsedExpression& parsedExpression, ExpressionType expressionType) {
    unique_ptr<LogicalExpression> logicalExpression;
    for (auto i = 0ul; i < parsedExpression.getNumChildren(); ++i) {
        auto next = bindExpression(parsedExpression.getChildren(i));
        validateExpectedType(*next, BOOL);
        logicalExpression = !logicalExpression ? move(next) :
                                                 make_unique<LogicalExpression>(expressionType,
                                                     BOOL, move(logicalExpression), move(next));
    }
    return logicalExpression;
}

unique_ptr<LogicalExpression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedLeft = parsedExpression.getChildren(0);
    auto& parsedRight = parsedExpression.getChildren(1);
    if (LITERAL_NULL == parsedLeft.getType() || LITERAL_NULL == parsedRight.getType()) {
        // rewrite == null as IS NULL
        if (EQUALS == parsedExpression.getType()) {
            return make_unique<LogicalExpression>(IS_NULL, BOOL,
                LITERAL_NULL == parsedLeft.getType() ? bindExpression(parsedRight) :
                                                       bindExpression(parsedLeft));
        }
        // rewrite <> null as IS NOT NULL
        if (NOT_EQUALS == parsedExpression.getType()) {
            return make_unique<LogicalExpression>(IS_NOT_NULL, BOOL,
                LITERAL_NULL == parsedLeft.getType() ? bindExpression(parsedRight) :
                                                       bindExpression(parsedLeft));
        }
    }
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(parsedLeft);
    auto right = bindExpression(parsedRight);
    isNumericalType(left->getDataType()) ? validateNumericalType(*right) :
                                           validateExpectedType(*right, left->getDataType());
    return make_unique<LogicalExpression>(
        parsedExpression.getType(), BOOL, move(left), move(right));
}

unique_ptr<LogicalExpression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(parsedExpression.getChildren(0));
    validateNumericalType(*left);
    auto right = bindExpression(parsedExpression.getChildren(1));
    validateNumericalType(*right);
    DataType resultType;
    if (DOUBLE == left->getDataType() || DOUBLE == right->getDataType()) {
        resultType = DOUBLE;
    } else if (INT64 == left->getDataType() || INT64 == right->getDataType()) {
        resultType = INT64;
    } else {
        resultType = INT32;
    }
    auto type = parsedExpression.getType();
    switch (type) {
    case ADD:
    case SUBTRACT:
    case MULTIPLY:
    case POWER:
        return make_unique<LogicalExpression>(type, resultType, move(left), move(right));
    case DIVIDE:
        return make_unique<LogicalExpression>(DIVIDE, DOUBLE, move(left), move(right));
    case MODULO:
        return make_unique<LogicalExpression>(MODULO, INT32, move(left), move(right));
    default:
        throw invalid_argument("Should never happen. Cannot bind expression type of " +
                               expressionTypeToString(type) + " as binary arithmetic expression.");
    }
}

unique_ptr<LogicalExpression> ExpressionBinder::bindUnaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto child = bindExpression(parsedExpression.getChildren(0));
    validateNumericalType(*child);
    return make_unique<LogicalExpression>(
        parsedExpression.getType(), child->getDataType(), move(child));
}

unique_ptr<LogicalExpression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(parsedExpression.getChildren(0));
    validateExpectedType(*left, STRING);
    auto right = bindExpression(parsedExpression.getChildren(1));
    validateExpectedType(*right, STRING);
    return make_unique<LogicalExpression>(
        parsedExpression.getType(), BOOL, move(left), move(right));
}

unique_ptr<LogicalExpression> ExpressionBinder::bindNullComparisonOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto childExpression = bindExpression(parsedExpression.getChildren(0));
    return make_unique<LogicalExpression>(parsedExpression.getType(), BOOL, move(childExpression));
}

// Note ParsedPropertyExpression is unary ,but LogicalPropertyExpression is leaf.
// Bind property to node or rel. This should change for unstructured properties.
unique_ptr<LogicalExpression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto propertyName = parsedExpression.getText();
    auto childExpression = bindExpression(parsedExpression.getChildren(0));
    if (NODE == childExpression->getDataType()) {
        auto nodeName = childExpression->getVariableName();
        auto nodeLabel = graphInScope.getQueryNode(nodeName)->getLabel();
        if (!catalog.containNodeProperty(nodeLabel, propertyName)) {
            throw invalid_argument(
                "Node: " + nodeName + " does not have property: " + propertyName);
        }
        return make_unique<LogicalExpression>(PROPERTY,
            catalog.getNodePropertyTypeFromString(nodeLabel, propertyName),
            nodeName + "." + propertyName);
    }
    if (REL == childExpression->getDataType()) {
        auto relName = childExpression->getVariableName();
        auto relLabel = graphInScope.getQueryRel(relName)->getLabel();
        if (!catalog.containRelProperty(relLabel, propertyName)) {
            throw invalid_argument("Rel: " + relName + " does not have property: " + propertyName);
        }
        return make_unique<LogicalExpression>(PROPERTY,
            catalog.getRelPropertyTypeFromString(relLabel, propertyName),
            relName + "." + propertyName);
    }
    throw invalid_argument(
        "Property: " + parsedExpression.getText() + " is not associated with any node or rel.");
}

unique_ptr<LogicalExpression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto literalVal = parsedExpression.getText();
    auto literalType = parsedExpression.getType();
    switch (literalType) {
    case LITERAL_INT:
        return make_unique<LogicalExpression>(LITERAL_INT, INT32, Literal(stoi(literalVal)));
    case LITERAL_DOUBLE:
        return make_unique<LogicalExpression>(LITERAL_DOUBLE, DOUBLE, Literal(stod(literalVal)));
    case LITERAL_BOOLEAN:
        return make_unique<LogicalExpression>(
            LITERAL_BOOLEAN, BOOL, Literal((uint8_t)("true" == literalVal)));
    case LITERAL_STRING:
        return make_unique<LogicalExpression>(
            LITERAL_STRING, STRING, Literal(literalVal.substr(1, literalVal.size() - 2)));
    default:
        throw invalid_argument("Cannot bind expression type of " +
                               expressionTypeToString(literalType) + " as literal expression");
    }
}

// Bind variable to either node or rel in the query graph
// Should change once we have WITH statement
unique_ptr<LogicalExpression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto varName = parsedExpression.getText();
    if (graphInScope.containsQueryNode(varName)) {
        return make_unique<LogicalExpression>(VARIABLE, NODE, varName);
    }
    if (graphInScope.containsQueryRel(varName)) {
        return make_unique<LogicalExpression>(VARIABLE, REL, varName);
    }
    throw invalid_argument("Variable: " + varName + " is not in scope.");
}

} // namespace planner
} // namespace graphflow
