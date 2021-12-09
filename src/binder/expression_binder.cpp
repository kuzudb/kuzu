#include "src/binder/include/expression_binder.h"

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/binder/include/expression/rel_expression.h"
#include "src/binder/include/query_binder.h"
#include "src/common/include/date.h"
#include "src/common/include/interval.h"
#include "src/common/include/timestamp.h"
#include "src/common/include/types.h"

namespace graphflow {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
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
    } else if (isExpressionLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    } else if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    } else if (EXISTENTIAL_SUBQUERY == expressionType) {
        expression = bindExistentialSubqueryExpression(parsedExpression);
    } else if (!expression) {
        throw invalid_argument(
            "Bind " + expressionTypeToString(expressionType) + " expression is not implemented.");
    }
    if (!parsedExpression.alias.empty()) {
        expression->setAlias(parsedExpression.alias);
    }
    expression->setRawName(parsedExpression.getRawName());
    if (isExpressionAggregate(expression->expressionType)) {
        validateAggregationExpressionIsNotNested(*expression);
    }
    return expression;
}

shared_ptr<Expression> ExpressionBinder::bindBinaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children[0]);
    auto right = bindExpression(*parsedExpression.children[1]);
    validateBoolOrUnstructured(*left);
    validateBoolOrUnstructured(*right);
    if (left->dataType == UNSTRUCTURED) {
        left = castUnstructuredToBool(move(left));
    }
    if (right->dataType == UNSTRUCTURED) {
        right = castUnstructuredToBool(move(right));
    }
    return make_shared<Expression>(parsedExpression.type, BOOL, left, right);
}

shared_ptr<Expression> ExpressionBinder::bindUnaryBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateBoolOrUnstructured(*child);
    if (child->dataType == UNSTRUCTURED) {
        child = castUnstructuredToBool(child);
    }
    return make_shared<Expression>(parsedExpression.type, BOOL, (child));
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children.at(0));
    auto right = bindExpression(*parsedExpression.children.at(1));
    if (left->dataType == UNSTRUCTURED || right->dataType == UNSTRUCTURED) {
        return make_shared<Expression>(parsedExpression.type, BOOL, move(left), move(right));
    }
    TypeUtils::isNumericalType(left->dataType) ?
        validateNumeric(*right) :
        validateExpectedDataType(*right, unordered_set<DataType>{left->dataType});
    auto expressionType = NODE_ID == left->dataType ?
                              comparisonToIDComparison(parsedExpression.type) :
                              parsedExpression.type;
    return make_shared<Expression>(expressionType, BOOL, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.children.at(0));
    auto right = bindExpression(*parsedExpression.children.at(1));
    if (left->dataType == UNSTRUCTURED || right->dataType == UNSTRUCTURED) {
        return make_shared<Expression>(
            parsedExpression.type, UNSTRUCTURED, move(left), move(right));
    } else if (left->dataType == STRING || right->dataType == STRING) {
        validateExpectedBinaryOperation(
            *left, *right, parsedExpression.type, unordered_set<ExpressionType>{ADD});
        if (left->dataType != STRING) {
            left = castExpressionToString(move(left));
        }
        if (right->dataType != STRING) {
            right = castExpressionToString(move(right));
        }
        return make_shared<Expression>(parsedExpression.type, STRING, move(left), move(right));
    } else if (left->dataType == DATE || right->dataType == DATE) {
        return bindBinaryDateArithmeticExpression(parsedExpression.type, move(left), move(right));
    } else if (left->dataType == TIMESTAMP || right->dataType == TIMESTAMP) {
        return bindBinaryTimestampArithmeticExpression(
            parsedExpression.type, move(left), move(right));
    } else if (left->dataType == INTERVAL || right->dataType == INTERVAL) {
        return bindBinaryIntervalArithmeticExpression(
            parsedExpression.type, move(left), move(right));
    }
    validateNumeric(*left);
    validateNumeric(*right);
    DataType resultType = left->dataType == DOUBLE || right->dataType == DOUBLE ? DOUBLE : INT64;
    return make_shared<Expression>(parsedExpression.type, resultType, move(left), move(right));
}

shared_ptr<Expression> ExpressionBinder::bindBinaryDateArithmeticExpression(
    ExpressionType expressionType, shared_ptr<Expression> left, shared_ptr<Expression> right) {
    if (left->dataType != DATE) {
        return bindBinaryDateArithmeticExpression(expressionType, move(right), move(left));
    }
    validateExpectedBinaryOperation(
        *left, *right, expressionType, unordered_set<ExpressionType>{ADD, SUBTRACT});
    if (expressionType == ADD) {
        // date + int → date
        // date + interval → date
        validateExpectedDataType(*right, unordered_set<DataType>{INT64, INTERVAL});
        return make_shared<Expression>(expressionType, DATE, move(left), move(right));
    } else if (expressionType == SUBTRACT) {
        // date - date → integer
        // date - integer → date
        // date - interval → date
        validateExpectedDataType(*right, unordered_set<DataType>{INT64, INTERVAL, DATE});
        auto resultType = right->dataType == DATE ? INT64 : DATE;
        return make_shared<Expression>(expressionType, resultType, move(left), move(right));
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindBinaryTimestampArithmeticExpression(
    ExpressionType expressionType, shared_ptr<Expression> left, shared_ptr<Expression> right) {
    if (left->dataType != TIMESTAMP) {
        return bindBinaryTimestampArithmeticExpression(expressionType, move(right), move(left));
    }
    validateExpectedBinaryOperation(
        *left, *right, expressionType, unordered_set<ExpressionType>{ADD, SUBTRACT});
    if (expressionType == ADD) {
        // timestamp + interval → timestamp
        validateExpectedDataType(*right, unordered_set<DataType>{INTERVAL});
        return make_shared<Expression>(expressionType, TIMESTAMP, move(left), move(right));
    } else if (expressionType == SUBTRACT) {
        // timestamp - interval → timestamp
        // timestamp - timestamp → interval
        validateExpectedDataType(*right, unordered_set<DataType>{INTERVAL, TIMESTAMP});
        auto resultType = right->dataType == INTERVAL ? TIMESTAMP : INTERVAL;
        return make_shared<Expression>(expressionType, resultType, move(left), move(right));
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindBinaryIntervalArithmeticExpression(
    ExpressionType expressionType, shared_ptr<Expression> left, shared_ptr<Expression> right) {
    if (left->dataType != INTERVAL) {
        return bindBinaryIntervalArithmeticExpression(expressionType, move(right), move(left));
    }
    validateExpectedBinaryOperation(
        *left, *right, expressionType, unordered_set<ExpressionType>{ADD, SUBTRACT, DIVIDE});
    if (expressionType == ADD || expressionType == SUBTRACT) {
        // interval +/- interval → interval
        validateExpectedDataType(*right, unordered_set<DataType>{INTERVAL});
        return make_shared<Expression>(expressionType, INTERVAL, move(left), move(right));
    } else if (expressionType == DIVIDE) {
        // interval / int → interval
        validateExpectedDataType(*right, unordered_set<DataType>{INT64});
        return make_shared<Expression>(expressionType, INTERVAL, move(left), move(right));
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindUnaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateNumericOrUnstructured(*child);
    return make_shared<Expression>(parsedExpression.type, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    auto left = bindExpression(*parsedExpression.children.at(0));
    auto right = bindExpression(*parsedExpression.children.at(1));
    validateStringOrUnstructured(*left);
    validateStringOrUnstructured(*right);
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
    if (!queryBinder->variablesInScope.contains(csvLineExtractExpressionName)) {
        throw invalid_argument("Variable " + csvVariableName + " not defined or idx out of bound.");
    }
    return queryBinder->variablesInScope.at(csvLineExtractExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindNullComparisonOperatorExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.children.at(0));
    return make_shared<Expression>(parsedExpression.type, BOOL, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    auto propertyName = parsedExpression.text;
    auto child = bindExpression(*parsedExpression.children.at(0));
    validateExpectedDataType(*child, unordered_set<DataType>{NODE, REL});
    auto& catalog = queryBinder->catalog;
    if (NODE == child->dataType) {
        auto node = static_pointer_cast<NodeExpression>(child);
        if (catalog.containNodeProperty(node->label, propertyName)) {
            auto& property = catalog.getNodeProperty(node->label, propertyName);
            return make_shared<PropertyExpression>(
                property.dataType, propertyName, property.id, move(child));
        } else {
            throw invalid_argument(
                "Node " + node->getRawName() + " does not have property " + propertyName + ".");
        }
    } else if (REL == child->dataType) {
        auto rel = static_pointer_cast<RelExpression>(child);
        if (catalog.containRelProperty(rel->label, propertyName)) {
            auto& property = catalog.getRelProperty(rel->label, propertyName);
            return make_shared<PropertyExpression>(
                property.dataType, propertyName, property.id, move(child));
        } else {
            throw invalid_argument(
                "Rel " + rel->getRawName() + " does not have property " + propertyName + ".");
        }
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto functionName = parsedExpression.text;
    StringUtils::toUpper(functionName);
    if (functionName == ABS_FUNC_NAME) {
        return bindAbsFunctionExpression(parsedExpression);
    } else if (functionName == COUNT_STAR_FUNC_NAME) {
        return bindCountStarFunctionExpression(parsedExpression);
    } else if (functionName == COUNT_FUNC_NAME) {
        return bindCountFunctionExpression(parsedExpression);
    } else if (functionName == SUM_FUNC_NAME) {
        return bindSumMinMaxFunctionExpression(parsedExpression, SUM_FUNC);
    } else if (functionName == AVG_FUNC_NAME) {
        return bindAvgFunctionExpression(parsedExpression);
    } else if (functionName == MIN_FUNC_NAME) {
        return bindSumMinMaxFunctionExpression(parsedExpression, MIN_FUNC);
    } else if (functionName == MAX_FUNC_NAME) {
        return bindSumMinMaxFunctionExpression(parsedExpression, MAX_FUNC);
    } else if (functionName == ID_FUNC_NAME) {
        return bindIDFunctionExpression(parsedExpression);
    } else if (functionName == DATE_FUNC_NAME) {
        return bindDateFunctionExpression(parsedExpression);
    } else if (functionName == TIMESTAMP_FUNC_NAME) {
        return bindTimestampFunctionExpression(parsedExpression);
    } else if (functionName == FLOOR_FUNC_NAME) {
        return bindFloorFunctionExpression(parsedExpression);
    } else if (functionName == CEIL_FUNC_NAME) {
        return bindCeilFunctionExpression(parsedExpression);
    } else if (functionName == INTERVAL_FUNC_NAME) {
        return bindIntervalFunctionExpression(parsedExpression);
    }
    throw invalid_argument(functionName + " function does not exist.");
}

shared_ptr<Expression> ExpressionBinder::bindFloorFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateNumericOrUnstructured(*child);
    return make_shared<Expression>(FLOOR_FUNC, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindCeilFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateNumericOrUnstructured(*child);
    return make_shared<Expression>(CEIL_FUNC, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindAbsFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateNumericOrUnstructured(*child);
    return make_shared<Expression>(ABS_FUNC, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindCountStarFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 0);
    return make_shared<Expression>(COUNT_STAR_FUNC, INT64,
        queryBinder->getUniqueExpressionName(parsedExpression.getRawName()));
}

shared_ptr<Expression> ExpressionBinder::bindCountFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    return make_shared<Expression>(COUNT_FUNC, INT64, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindAvgFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateNumericOrUnstructured(*child);
    return make_shared<Expression>(AVG_FUNC, DOUBLE, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindSumMinMaxFunctionExpression(
    const ParsedExpression& parsedExpression, ExpressionType expressionType) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    return make_shared<Expression>(expressionType, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindIDFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateExpectedDataType(*child, unordered_set<DataType>{NODE});
    return make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id */, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindDateFunctionExpression(
    const ParsedExpression& parsedExpression) {
    return bindStringCastingFunctionExpression<date_t>(
        parsedExpression, CAST_STRING_TO_DATE, LITERAL_DATE, DATE, Date::FromCString);
}

shared_ptr<Expression> ExpressionBinder::bindTimestampFunctionExpression(
    const ParsedExpression& parsedExpression) {
    return bindStringCastingFunctionExpression<timestamp_t>(parsedExpression,
        CAST_STRING_TO_TIMESTAMP, LITERAL_TIMESTAMP, TIMESTAMP, Timestamp::FromCString);
}

shared_ptr<Expression> ExpressionBinder::bindIntervalFunctionExpression(
    const ParsedExpression& parsedExpression) {
    return bindStringCastingFunctionExpression<interval_t>(parsedExpression,
        CAST_STRING_TO_INTERVAL, LITERAL_INTERVAL, INTERVAL, Interval::FromCString);
}

template<typename T>
shared_ptr<Expression> ExpressionBinder::bindStringCastingFunctionExpression(
    const ParsedExpression& parsedExpression, ExpressionType castExpressionType,
    ExpressionType literalExpressionType, DataType resultDataType,
    std::function<T(const char*, uint64_t)> castFunction) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.children[0]);
    validateStringOrUnstructured(*child);
    if (child->expressionType == LITERAL_STRING) {
        auto literalVal = static_pointer_cast<LiteralExpression>(child)->literal.strVal;
        auto literal = Literal(castFunction(literalVal.c_str(), literalVal.length()));
        return make_shared<LiteralExpression>(literalExpressionType, resultDataType, literal);
    }
    return make_shared<Expression>(castExpressionType, resultDataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto literalVal = parsedExpression.text;
    auto literalType = parsedExpression.type;
    switch (literalType) {
    case LITERAL_INT:
        return make_shared<LiteralExpression>(
            LITERAL_INT, INT64, Literal(TypeUtils::convertToInt64(literalVal.c_str())));
    case LITERAL_DOUBLE:
        return make_shared<LiteralExpression>(
            LITERAL_DOUBLE, DOUBLE, Literal(TypeUtils::convertToDouble(literalVal.c_str())));
    case LITERAL_BOOLEAN:
        return make_shared<LiteralExpression>(
            LITERAL_BOOLEAN, BOOL, Literal(TypeUtils::convertToBoolean(literalVal.c_str())));
    case LITERAL_STRING:
        return make_shared<LiteralExpression>(
            LITERAL_STRING, STRING, Literal(literalVal.substr(1, literalVal.size() - 2)));
    default:
        throw invalid_argument("Literal " + parsedExpression.getRawName() + "is not defined.");
    }
}

shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto variableName = parsedExpression.text;
    if (queryBinder->variablesInScope.contains(variableName)) {
        return queryBinder->variablesInScope.at(variableName);
    }
    throw invalid_argument("Variable " + parsedExpression.getRawName() + " not defined.");
}

shared_ptr<Expression> ExpressionBinder::bindExistentialSubqueryExpression(
    const ParsedExpression& parsedExpression) {
    auto prevVariablesInScope = queryBinder->enterSubquery();
    auto boundSingleQuery = queryBinder->bind(*parsedExpression.subquery);
    queryBinder->exitSubquery(move(prevVariablesInScope));
    return make_shared<ExistentialSubqueryExpression>(move(boundSingleQuery),
        queryBinder->getUniqueExpressionName(parsedExpression.getRawName()));
}

shared_ptr<Expression> ExpressionBinder::castUnstructuredToBool(shared_ptr<Expression> expression) {
    assert(expression->dataType == UNSTRUCTURED);
    return make_shared<Expression>(CAST_UNSTRUCTURED_TO_BOOL_VALUE, BOOL, move(expression));
}

shared_ptr<Expression> ExpressionBinder::castExpressionToString(shared_ptr<Expression> expression) {
    if (isExpressionLiteral(expression->expressionType)) {
        static_pointer_cast<LiteralExpression>(expression)->castToString();
        return expression;
    }
    return make_shared<Expression>(CAST_TO_STRING, STRING, move(expression));
}

void ExpressionBinder::validateNoNullLiteralChildren(const ParsedExpression& parsedExpression) {
    for (auto& child : parsedExpression.children) {
        if (LITERAL_NULL == child->type) {
            throw invalid_argument("Expression " + parsedExpression.getRawName() +
                                   " cannot have null literal children.");
        }
    }
}

void ExpressionBinder::validateNumberOfChildren(
    const ParsedExpression& parsedExpression, uint32_t expectedNumChildren) {
    GF_ASSERT(parsedExpression.type == FUNCTION);
    if (parsedExpression.children.size() != expectedNumChildren) {
        throw invalid_argument("Expected " + to_string(expectedNumChildren) + " parameters for " +
                               parsedExpression.text + " function but get " +
                               to_string(parsedExpression.children.size()) + ".");
    }
}

void ExpressionBinder::validateExpectedDataType(
    const Expression& expression, const unordered_set<DataType>& expectedTypes) {
    auto dataType = expression.dataType;
    if (!expectedTypes.contains(dataType)) {
        string expectedTypesStr;
        vector<DataType> expectedTypesVec{expectedTypes.begin(), expectedTypes.end()};
        auto numExpectedTypes = expectedTypes.size();
        for (auto i = 0u; i < numExpectedTypes - 1; ++i) {
            expectedTypesStr += TypeUtils::dataTypeToString(expectedTypesVec[i]) + ", ";
        }
        expectedTypesStr += TypeUtils::dataTypeToString(expectedTypesVec[numExpectedTypes - 1]);
        throw invalid_argument(expression.getRawName() + " has data type " +
                               TypeUtils::dataTypeToString(dataType) + ". " + expectedTypesStr +
                               " was expected.");
    }
}

void ExpressionBinder::validateExpectedBinaryOperation(const Expression& left,
    const Expression& right, ExpressionType type,
    const unordered_set<ExpressionType>& expectedTypes) {
    if (!expectedTypes.contains(type)) {
        throw invalid_argument("Operation " + expressionTypeToString(type) + " between " +
                               left.getRawName() + " and " + right.getRawName() +
                               " is not supported.");
    }
}

void ExpressionBinder::validateAggregationExpressionIsNotNested(const Expression& expression) {
    if (expression.expressionType == COUNT_STAR_FUNC) {
        return;
    }
    if (expression.getChild(0)->hasAggregationExpression()) {
        throw invalid_argument(
            "Expression " + expression.getRawName() + " contains nested aggregation.");
    }
}

} // namespace binder
} // namespace graphflow
