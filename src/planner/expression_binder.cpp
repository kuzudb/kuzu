#include "src/planner/include/expression_binder.h"

#include "src/expression/include/logical/logical_literal_expression.h"
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
        expression = make_shared<LogicalExpression>(
            CAST_UNSTRUCTURED_VECTOR_TO_BOOL_VECTOR, BOOL, move(expression));
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
            return make_shared<LogicalLiteralExpression>(LITERAL_BOOLEAN, BOOL, Value(FALSE));
        } else {
            return make_shared<LogicalLiteralExpression>(LITERAL_BOOLEAN, BOOL, Value(NULL_BOOL));
        }
    }
    auto left = bindExpression(parsedLeft);
    auto right = bindExpression(parsedRight);
    if (left->dataType != UNSTRUCTURED && right->dataType == UNSTRUCTURED) {
        if (isExpressionLeafLiteral(left->expressionType)) {
            left->cast(UNSTRUCTURED);
        } else {
            left = make_shared<LogicalExpression>(
                CAST_TO_UNSTRUCTURED_VECTOR, UNSTRUCTURED, move(left));
        }
    } else if (left->dataType == UNSTRUCTURED && right->dataType != UNSTRUCTURED) {
        if (isExpressionLeafLiteral(right->expressionType)) {
            right->cast(UNSTRUCTURED);
        } else {
            right = make_shared<LogicalExpression>(
                CAST_TO_UNSTRUCTURED_VECTOR, UNSTRUCTURED, move(right));
        }
        return make_shared<LogicalExpression>(parsedExpression.type, BOOL, move(left), move(right));
    }
    if (isNumericalType(left->dataType)) {
        if (!isNumericalType(right->dataType)) {
            return make_shared<LogicalLiteralExpression>(LITERAL_BOOLEAN, BOOL, Value(NULL_BOOL));
        }
    } else if (left->dataType != right->dataType) {
        return make_shared<LogicalLiteralExpression>(LITERAL_BOOLEAN, BOOL, Value(NULL_BOOL));
    }
    return make_shared<LogicalExpression>(NODE_ID == left->dataType ?
                                              comparisonToIDComparison(parsedExpression.type) :
                                              parsedExpression.type,
        BOOL, move(left), move(right));
}

shared_ptr<LogicalExpression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    auto right = bindExpression(*parsedExpression.children.at(1));
    if (parsedExpression.type == ADD) {
        if (left->dataType == STRING || right->dataType == STRING) {
            if (left->dataType != STRING) {
                if (isExpressionLeafLiteral(left->expressionType)) {
                    left->cast(STRING);
                } else {
                    left = make_shared<LogicalExpression>(CAST_TO_STRING, STRING, move(left));
                }
            }
            if (right->dataType != STRING) {
                if (isExpressionLeafLiteral(right->expressionType)) {
                    right->cast(STRING);
                } else {
                    right = make_shared<LogicalExpression>(CAST_TO_STRING, STRING, move(right));
                }
            }
            return make_shared<LogicalExpression>(
                parsedExpression.type, STRING, move(left), move(right));
        }
    }
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

shared_ptr<LogicalExpression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto propertyName = parsedExpression.text;
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    if (NODE == childExpression->dataType) {
        auto node = static_pointer_cast<LogicalNodeExpression>(childExpression);
        if (!catalog.containNodeProperty(node->label, propertyName) &&
            !catalog.containUnstrNodeProperty(node->label, propertyName)) {
            throw invalid_argument("Node " + node->getAliasElseRawExpression() +
                                   " does not have property " + propertyName + ".");
        }
        auto dataType = catalog.containNodeProperty(node->label, propertyName) ?
                            catalog.getNodePropertyTypeFromString(node->label, propertyName) :
                            UNSTRUCTURED;
        return make_shared<LogicalExpression>(
            PROPERTY, dataType, node->variableName + "." + propertyName);
    }
    if (REL == childExpression->dataType) {
        auto rel = static_pointer_cast<LogicalRelExpression>(childExpression);
        if (!catalog.containRelProperty(rel->label, propertyName)) {
            throw invalid_argument("Rel " + rel->getAliasElseRawExpression() +
                                   " does not have property " + propertyName + ".");
        }
        return make_shared<LogicalExpression>(PROPERTY,
            catalog.getRelPropertyTypeFromString(rel->label, propertyName),
            rel->variableName + "." + propertyName);
    }
    throw invalid_argument("Type mismatch: expect NODE or REL, but " +
                           childExpression->rawExpression + " was " +
                           dataTypeToString(childExpression->dataType) + ".");
}

// only support COUNT(*) or ID(nodeVariable)
shared_ptr<LogicalExpression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto functionName = parsedExpression.text;
    transform(begin(functionName), end(functionName), begin(functionName), ::toupper);
    if (FUNCTION_COUNT_STAR == functionName) {
        return make_shared<LogicalExpression>(FUNCTION, INT64, FUNCTION_COUNT_STAR);
    } else if (FUNCTION_ID == functionName) {
        if (1 != parsedExpression.children.size()) {
            throw invalid_argument(functionName + " takes exactly one parameter.");
        }
        auto child = bindExpression(*parsedExpression.children[0]);
        if (NODE != child->dataType) {
            throw invalid_argument("Expect " + child->rawExpression + " to be a node, but it was " +
                                   dataTypeToString(child->dataType));
        }
        return make_shared<LogicalExpression>(
            PROPERTY, NODE_ID, static_pointer_cast<LogicalNodeExpression>(child)->getIDProperty());
    } else {
        throw invalid_argument(functionName + " is not supported.");
    }
}

shared_ptr<LogicalExpression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto literalVal = parsedExpression.text;
    auto literalType = parsedExpression.type;
    switch (literalType) {
    case LITERAL_INT:
        return make_shared<LogicalLiteralExpression>(LITERAL_INT, INT32, Value(stoi(literalVal)));
    case LITERAL_DOUBLE:
        return make_shared<LogicalLiteralExpression>(
            LITERAL_DOUBLE, DOUBLE, Value(stod(literalVal)));
    case LITERAL_BOOLEAN:
        return make_shared<LogicalLiteralExpression>(
            LITERAL_BOOLEAN, BOOL, Value((uint8_t)("true" == literalVal)));
    case LITERAL_STRING:
        return make_shared<LogicalLiteralExpression>(
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
        throw invalid_argument(logicalExpression.rawExpression + " has data type " +
                               dataTypeToString(dataType) + ". " + dataTypeToString(expectedType) +
                               " was expected.");
    }
}

void validateNumericalType(const LogicalExpression& logicalExpression) {
    auto dataType = logicalExpression.dataType;
    if (!isNumericalType(dataType)) {
        throw invalid_argument(logicalExpression.rawExpression + " has data type " +
                               dataTypeToString(dataType) +
                               ". A numerical data type was expected.");
    }
}

} // namespace planner
} // namespace graphflow
