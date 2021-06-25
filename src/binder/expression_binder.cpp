#include "src/binder/include/expression_binder.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/binder/include/expression/rel_expression.h"

namespace graphflow {
namespace binder {

static void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression);
static void validateExpectedType(const Expression& expression, DataType expectedType);
static void validateNumericalType(const Expression& expression);

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    shared_ptr<Expression> expression;
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
    } else if (CSV_LINE_EXTRACT == expressionType) {
        expression = bindCSVLineExtractExpression(parsedExpression);
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

static shared_ptr<Expression> validateAsBoolAndCastIfNecessary(shared_ptr<Expression> expression) {
    if (expression->dataType != UNSTRUCTURED) {
        validateExpectedType(*expression, BOOL);
    } else {
        assert(expression->dataType == UNSTRUCTURED);
        expression = make_shared<Expression>(
            CAST_UNSTRUCTURED_VECTOR_TO_BOOL_VECTOR, BOOL, move(expression));
    }
    return expression;
}

shared_ptr<Expression> ExpressionBinder::bindBinaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children[0]);
    auto right = bindExpression(*parsedExpression.children[1]);
    return make_shared<Expression>(parsedExpression.type, BOOL,
        validateAsBoolAndCastIfNecessary(left), validateAsBoolAndCastIfNecessary(right));
}

shared_ptr<Expression> ExpressionBinder::bindUnaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    return make_shared<Expression>(NOT, BOOL, validateAsBoolAndCastIfNecessary(child));
}

static shared_ptr<Expression> castToUnstructured(shared_ptr<Expression> expression) {
    if (isExpressionLeafLiteral(expression->expressionType)) {
        expression->cast(UNSTRUCTURED);
    } else {
        expression =
            make_shared<Expression>(CAST_TO_UNSTRUCTURED_VECTOR, UNSTRUCTURED, move(expression));
    }
    return expression;
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedLeft = *parsedExpression.children.at(0);
    auto& parsedRight = *parsedExpression.children.at(1);
    if (parsedLeft.type == LITERAL_NULL || parsedRight.type == LITERAL_NULL) {
        if (parsedExpression.type == EQUALS || parsedExpression.type == NOT_EQUALS) {
            return make_shared<LiteralExpression>(LITERAL_BOOLEAN, BOOL, Literal(FALSE));
        } else {
            return make_shared<LiteralExpression>(LITERAL_BOOLEAN, BOOL, Literal(NULL_BOOL));
        }
    }
    auto left = bindExpression(parsedLeft);
    auto right = bindExpression(parsedRight);
    if (left->dataType == UNSTRUCTURED || right->dataType == UNSTRUCTURED) {
        if (left->dataType != UNSTRUCTURED) {
            left = castToUnstructured(move(left));
        } else if (right->dataType != UNSTRUCTURED) {
            right = castToUnstructured(move(right));
        }
        return make_shared<Expression>(parsedExpression.type, BOOL, move(left), move(right));
    }
    if (isNumericalType(left->dataType)) {
        if (!isNumericalType(right->dataType)) {
            return make_shared<LiteralExpression>(LITERAL_BOOLEAN, BOOL, Literal(NULL_BOOL));
        }
    } else if (left->dataType != right->dataType) {
        return make_shared<LiteralExpression>(LITERAL_BOOLEAN, BOOL, Literal(NULL_BOOL));
    }
    return make_shared<Expression>(NODE_ID == left->dataType ?
                                       comparisonToIDComparison(parsedExpression.type) :
                                       parsedExpression.type,
        BOOL, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    auto right = bindExpression(*parsedExpression.children.at(1));
    if (left->dataType == UNSTRUCTURED || right->dataType == UNSTRUCTURED) {
        if (left->dataType != UNSTRUCTURED) {
            left = castToUnstructured(move(left));
        } else if (right->dataType != UNSTRUCTURED) {
            right = castToUnstructured(move(right));
        }
        return make_shared<Expression>(
            parsedExpression.type, UNSTRUCTURED, move(left), move(right));
    }
    if (parsedExpression.type == ADD) {
        if (left->dataType == STRING || right->dataType == STRING) {
            if (left->dataType != STRING) {
                if (isExpressionLeafLiteral(left->expressionType)) {
                    left->cast(STRING);
                } else {
                    left = make_shared<Expression>(CAST_TO_STRING, STRING, move(left));
                }
            }
            if (right->dataType != STRING) {
                if (isExpressionLeafLiteral(right->expressionType)) {
                    right->cast(STRING);
                } else {
                    right = make_shared<Expression>(CAST_TO_STRING, STRING, move(right));
                }
            }
            return make_shared<Expression>(parsedExpression.type, STRING, move(left), move(right));
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
    return make_shared<Expression>(parsedExpression.type, resultType, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindUnaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateNumericalType(*child);
    return make_shared<Expression>(parsedExpression.type, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    validateExpectedType(*left, STRING);
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateExpectedType(*right, STRING);
    return make_shared<Expression>(parsedExpression.type, BOOL, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindCSVLineExtractExpression(
    const ParsedExpression& parsedExpression) {
    auto idxExpression = bindExpression(*parsedExpression.children[1]);
    if (LITERAL_INT != idxExpression->expressionType) {
        throw invalid_argument("LIST EXTRACT INDEX must be LITERAL_INT.");
    }
    auto csvVariableName = parsedExpression.children[0]->text;
    auto csvLineExtractExpressionName =
        csvVariableName + "[" + ((LiteralExpression&)*idxExpression).literal.toString() + "]";
    if (!variablesInScope.contains(csvLineExtractExpressionName)) {
        throw invalid_argument("Variable " + csvVariableName + " not defined or idx out of bound.");
    }
    return variablesInScope.at(csvLineExtractExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindNullComparisonOperatorExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    return make_shared<Expression>(parsedExpression.type, BOOL, move(childExpression));
}

shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto propertyName = parsedExpression.text;
    auto childExpression = bindExpression(*parsedExpression.children.at(0));
    if (NODE == childExpression->dataType) {
        auto node = static_pointer_cast<NodeExpression>(childExpression);
        auto propertyVariableName = node->variableName + "." + propertyName;
        if (catalog.containNodeProperty(node->label, propertyName)) {
            auto dataType = catalog.getNodePropertyTypeFromString(node->label, propertyName);
            auto propertyKey = catalog.getNodePropertyKeyFromString(node->label, propertyName);
            return make_shared<PropertyExpression>(
                propertyVariableName, dataType, propertyKey, move(childExpression));
        } else if (catalog.containUnstrNodeProperty(node->label, propertyName)) {
            auto propertyKey = catalog.getUnstrNodePropertyKeyFromString(node->label, propertyName);
            return make_shared<PropertyExpression>(
                propertyVariableName, UNSTRUCTURED, propertyKey, move(childExpression));
        } else {
            throw invalid_argument("Node " + node->getAliasElseRawExpression() +
                                   " does not have property " + propertyName + ".");
        }
    }
    if (REL == childExpression->dataType) {
        auto rel = static_pointer_cast<RelExpression>(childExpression);
        auto propertyVariableName = rel->variableName + "." + propertyName;
        if (catalog.containRelProperty(rel->label, propertyName)) {
            auto dataType = catalog.getRelPropertyTypeFromString(rel->label, propertyName);
            auto propertyKey = catalog.getRelPropertyKeyFromString(rel->label, propertyName);
            return make_shared<PropertyExpression>(
                propertyVariableName, dataType, propertyKey, move(childExpression));
        } else {
            throw invalid_argument("Rel " + rel->getAliasElseRawExpression() +
                                   " does not have property " + propertyName + ".");
        }
    }
    throw invalid_argument("Type mismatch: expect NODE or REL, but " +
                           childExpression->rawExpression + " was " +
                           dataTypeToString(childExpression->dataType) + ".");
}

// only support COUNT(*) or ID(nodeVariable)
shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto functionName = parsedExpression.text;
    transform(begin(functionName), end(functionName), begin(functionName), ::toupper);
    if (FUNCTION_COUNT_STAR == functionName) {
        return make_shared<Expression>(FUNCTION, INT64, FUNCTION_COUNT_STAR);
    } else if (FUNCTION_ID == functionName) {
        if (1 != parsedExpression.children.size()) {
            throw invalid_argument(functionName + " takes exactly one parameter.");
        }
        auto child = bindExpression(*parsedExpression.children[0]);
        if (NODE != child->dataType) {
            throw invalid_argument("Expect " + child->rawExpression + " to be a node, but it was " +
                                   dataTypeToString(child->dataType));
        }
        auto node = static_pointer_cast<NodeExpression>(child);
        return make_shared<PropertyExpression>(
            node->getIDProperty(), NODE_ID, UINT32_MAX, move(child));
    } else {
        throw invalid_argument(functionName + " is not supported.");
    }
}

shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto literalVal = parsedExpression.text;
    auto literalType = parsedExpression.type;
    switch (literalType) {
    case LITERAL_INT:
        return make_shared<LiteralExpression>(LITERAL_INT, INT32, Literal(stoi(literalVal)));
    case LITERAL_DOUBLE:
        return make_shared<LiteralExpression>(LITERAL_DOUBLE, DOUBLE, Literal(stod(literalVal)));
    case LITERAL_BOOLEAN:
        return make_shared<LiteralExpression>(
            LITERAL_BOOLEAN, BOOL, Literal((uint8_t)("true" == literalVal)));
    case LITERAL_STRING:
        return make_shared<LiteralExpression>(
            LITERAL_STRING, STRING, Literal(literalVal.substr(1, literalVal.size() - 2)));
    default:
        throw invalid_argument("Literal " + parsedExpression.rawExpression + "is not defined.");
    }
}

shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
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

void validateExpectedType(const Expression& expression, DataType expectedType) {
    auto dataType = expression.dataType;
    if (expectedType != dataType) {
        throw invalid_argument(expression.rawExpression + " has data type " +
                               dataTypeToString(dataType) + ". " + dataTypeToString(expectedType) +
                               " was expected.");
    }
}

void validateNumericalType(const Expression& expression) {
    auto dataType = expression.dataType;
    if (!isNumericalType(dataType)) {
        throw invalid_argument(expression.rawExpression + " has data type " +
                               dataTypeToString(dataType) +
                               ". A numerical data type was expected.");
    }
}

} // namespace binder
} // namespace graphflow
