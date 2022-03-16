#include "include/expression_binder.h"

#include "src/binder/expression/include/existential_subquery_expression.h"
#include "src/binder/expression/include/function_expression.h"
#include "src/binder/expression/include/literal_expression.h"
#include "src/binder/expression/include/rel_expression.h"
#include "src/binder/include/query_binder.h"
#include "src/common/types/include/type_utils.h"
#include "src/function/boolean/include/vector_boolean_operations.h"
#include "src/function/comparison/include/vector_comparison_operations.h"
#include "src/function/null/include/vector_null_operations.h"
#include "src/function/string/include/vector_string_operations.h"
#include "src/parser/expression/include/parsed_function_expression.h"
#include "src/parser/expression/include/parsed_literal_expression.h"
#include "src/parser/expression/include/parsed_property_expression.h"
#include "src/parser/expression/include/parsed_subquery_expression.h"
#include "src/parser/expression/include/parsed_variable_expression.h"

using namespace graphflow::function;

namespace graphflow {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    shared_ptr<Expression> expression;
    auto expressionType = parsedExpression.getExpressionType();
    if (isExpressionBoolConnection(expressionType)) {
        expression = bindBooleanExpression(parsedExpression);
    } else if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
    } else if (isExpressionArithmetic(expressionType)) {
        expression = isExpressionUnary(expressionType) ?
                         bindUnaryArithmeticExpression(parsedExpression) :
                         bindBinaryArithmeticExpression(parsedExpression);
    } else if (isExpressionStringOperator(expressionType)) {
        expression = bindStringOperatorExpression(parsedExpression);
    } else if (isExpressionNullOperator(expressionType)) {
        expression = bindNullOperatorExpression(parsedExpression);
    } else if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (isExpressionLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    } else if (VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    } else if (EXISTENTIAL_SUBQUERY == expressionType) {
        expression = bindExistentialSubqueryExpression(parsedExpression);
    } else if (!expression) {
        throw invalid_argument(
            "Bind " + expressionTypeToString(expressionType) + " expression is not implemented.");
    }
    if (parsedExpression.hasAlias()) {
        expression->setAlias(parsedExpression.getAlias());
    }
    expression->setRawName(parsedExpression.getRawName());
    if (isExpressionAggregate(expression->expressionType)) {
        validateAggregationExpressionIsNotNested(*expression);
    } else if (expression->expressionType == EXISTENTIAL_SUBQUERY) {
        validateExistsSubqueryHasNoAggregationOrOrderBy(*expression);
    }
    return expression;
}

shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        if (child->dataType == UNSTRUCTURED) {
            child = castUnstructuredToBool(move(child));
        }
        children.push_back(move(child));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto execFunc = VectorBooleanOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorBooleanOperations::bindSelectFunction(expressionType, children);
    return make_shared<ScalarFunctionExpression>(
        expressionType, BOOL, move(children), move(execFunc), move(selectFunc));
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    assert(parsedExpression.getNumChildren() == 2);
    auto left = bindExpression(*parsedExpression.getChild(0));
    auto right = bindExpression(*parsedExpression.getChild(1));
    if (left->dataType == UNSTRUCTURED && right->dataType != UNSTRUCTURED) {
        right = castToUnstructured(move(right));
    }
    if (left->dataType != UNSTRUCTURED && right->dataType == UNSTRUCTURED) {
        left = castToUnstructured(move(left));
    }
    expression_vector children;
    children.push_back(move(left));
    children.push_back(move(right));
    auto expressionType = parsedExpression.getExpressionType();
    auto execFunc = VectorComparisonOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorComparisonOperations::bindSelectFunction(expressionType, children);
    return make_shared<ScalarFunctionExpression>(
        expressionType, BOOL, move(children), move(execFunc), move(selectFunc));
}

shared_ptr<Expression> ExpressionBinder::bindBinaryArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    auto left = bindExpression(*parsedExpression.getChild(0));
    auto right = bindExpression(*parsedExpression.getChild(1));
    if (left->dataType == UNSTRUCTURED || right->dataType == UNSTRUCTURED) {
        return make_shared<Expression>(
            parsedExpression.getExpressionType(), UNSTRUCTURED, move(left), move(right));
    } else if (left->dataType == STRING || right->dataType == STRING) {
        validateExpectedBinaryOperation(*left, *right, parsedExpression.getExpressionType(),
            unordered_set<ExpressionType>{ADD});
        if (left->dataType != STRING) {
            left = castExpressionToString(move(left));
        }
        if (right->dataType != STRING) {
            right = castExpressionToString(move(right));
        }
        expression_vector children;
        children.push_back(left);
        children.push_back(right);
        auto execFunc = VectorStringOperations::bindExecFunction(STRING_CONCAT, children);
        return make_shared<ScalarFunctionExpression>(
            STRING_CONCAT, STRING, move(children), move(execFunc));
    } else if (left->dataType == DATE || right->dataType == DATE) {
        return bindBinaryDateArithmeticExpression(
            parsedExpression.getExpressionType(), move(left), move(right));
    } else if (left->dataType == TIMESTAMP || right->dataType == TIMESTAMP) {
        return bindBinaryTimestampArithmeticExpression(
            parsedExpression.getExpressionType(), move(left), move(right));
    } else if (left->dataType == INTERVAL || right->dataType == INTERVAL) {
        return bindBinaryIntervalArithmeticExpression(
            parsedExpression.getExpressionType(), move(left), move(right));
    }
    validateNumeric(*left);
    validateNumeric(*right);
    DataType resultType = left->dataType == DOUBLE || right->dataType == DOUBLE ? DOUBLE : INT64;
    return make_shared<Expression>(
        parsedExpression.getExpressionType(), resultType, move(left), move(right));
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
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateNumericOrUnstructured(*child);
    return make_shared<Expression>(
        parsedExpression.getExpressionType(), child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        // TODO: add cast unstructured to string
        children.push_back(move(child));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto execFunc = VectorStringOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorStringOperations::bindSelectFunction(expressionType, children);
    return make_shared<ScalarFunctionExpression>(
        expressionType, BOOL, move(children), move(execFunc), move(selectFunc));
}

shared_ptr<Expression> ExpressionBinder::bindNullOperatorExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto execFunc = VectorNullOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorNullOperations::bindSelectFunction(expressionType, children);
    return make_shared<ScalarFunctionExpression>(
        expressionType, BOOL, move(children), move(execFunc), move(selectFunc));
}

shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    auto& propertyExpression = (ParsedPropertyExpression&)parsedExpression;
    auto propertyName = propertyExpression.getPropertyName();
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, unordered_set<DataType>{NODE, REL});
    auto& catalog = queryBinder->catalog;
    if (NODE == child->dataType) {
        auto node = static_pointer_cast<NodeExpression>(child);
        if (catalog.containNodeProperty(node->getLabel(), propertyName)) {
            auto& property = catalog.getNodeProperty(node->getLabel(), propertyName);
            return make_shared<PropertyExpression>(
                property.dataType, propertyName, property.id, move(child));
        } else {
            throw invalid_argument(
                "Node " + node->getRawName() + " does not have property " + propertyName + ".");
        }
    } else if (REL == child->dataType) {
        auto rel = static_pointer_cast<RelExpression>(child);
        if (catalog.containRelProperty(rel->getLabel(), propertyName)) {
            auto& property = catalog.getRelProperty(rel->getLabel(), propertyName);
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
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    auto functionName = parsedFunctionExpression.getFunctionName();
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
    } else if (functionName == LIST_CREATION_FUNC_NAME) {
        return bindListCreationFunction(parsedExpression);
    }
    throw invalid_argument(functionName + " function does not exist.");
}

shared_ptr<Expression> ExpressionBinder::bindFloorFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateNumericOrUnstructured(*child);
    return make_shared<FunctionExpression>(FLOOR_FUNC, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindCeilFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateNumericOrUnstructured(*child);
    return make_shared<FunctionExpression>(CEIL_FUNC, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindAbsFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateNumericOrUnstructured(*child);
    return make_shared<FunctionExpression>(ABS_FUNC, child->dataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindCountStarFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 0);
    return make_shared<FunctionExpression>(COUNT_STAR_FUNC, INT64,
        queryBinder->getUniqueExpressionName(parsedExpression.getRawName()));
}

shared_ptr<Expression> ExpressionBinder::bindCountFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    return make_shared<FunctionExpression>(
        COUNT_FUNC, INT64, move(child), parsedFunctionExpression.getIsDistinct());
}

shared_ptr<Expression> ExpressionBinder::bindAvgFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateNumericOrUnstructured(*child);
    return make_shared<FunctionExpression>(
        AVG_FUNC, DOUBLE, move(child), parsedFunctionExpression.getIsDistinct());
}

shared_ptr<Expression> ExpressionBinder::bindSumMinMaxFunctionExpression(
    const ParsedExpression& parsedExpression, ExpressionType expressionType) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    return make_shared<FunctionExpression>(
        expressionType, child->dataType, move(child), parsedFunctionExpression.getIsDistinct());
}

shared_ptr<Expression> ExpressionBinder::bindIDFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
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

shared_ptr<Expression> ExpressionBinder::bindListCreationFunction(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    if (!children.empty()) {
        auto expectedChildDataType = children[0]->dataType;
        for (auto& child : children) {
            validateExpectedDataType(*child, unordered_set<DataType>{expectedChildDataType});
        }
    }
    return make_shared<Expression>(LIST_CREATION, LIST, move(children));
}

template<typename T>
shared_ptr<Expression> ExpressionBinder::bindStringCastingFunctionExpression(
    const ParsedExpression& parsedExpression, ExpressionType castExpressionType,
    ExpressionType literalExpressionType, DataType resultDataType,
    std::function<T(const char*, uint64_t)> castFunction) {
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateStringOrUnstructured(*child);
    if (child->expressionType == LITERAL_STRING) {
        auto literalVal = static_pointer_cast<LiteralExpression>(child)->literal.strVal;
        auto literal = Literal(castFunction(literalVal.c_str(), literalVal.length()));
        return make_shared<LiteralExpression>(literalExpressionType, resultDataType, literal);
    }
    return make_shared<FunctionExpression>(castExpressionType, resultDataType, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto& literalExpression = (ParsedLiteralExpression&)parsedExpression;
    auto literalVal = literalExpression.getValInStr();
    auto literalType = parsedExpression.getExpressionType();
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
        return make_shared<LiteralExpression>(LITERAL_STRING, STRING,
            Literal(literalVal.substr(1, literalVal.size() - 2) /* strip double quotation */));
    default:
        throw invalid_argument("Literal " + parsedExpression.getRawName() + "is not defined.");
    }
}

shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto& variableExpression = (ParsedVariableExpression&)parsedExpression;
    auto variableName = variableExpression.getVariableName();
    if (queryBinder->variablesInScope.contains(variableName)) {
        return queryBinder->variablesInScope.at(variableName);
    }
    throw invalid_argument("Variable " + parsedExpression.getRawName() + " not defined.");
}

shared_ptr<Expression> ExpressionBinder::bindExistentialSubqueryExpression(
    const ParsedExpression& parsedExpression) {
    auto& subqueryExpression = (ParsedSubqueryExpression&)parsedExpression;
    auto prevVariablesInScope = queryBinder->enterSubquery();
    auto boundSingleQuery = queryBinder->bindSingleQuery(*subqueryExpression.getSubquery());
    queryBinder->exitSubquery(move(prevVariablesInScope));
    return make_shared<ExistentialSubqueryExpression>(
        QueryNormalizer::normalizeQuery(*boundSingleQuery),
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

shared_ptr<Expression> ExpressionBinder::castToUnstructured(shared_ptr<Expression> expression) {
    return make_shared<Expression>(CAST_TO_UNSTRUCTURED_VALUE, UNSTRUCTURED, move(expression));
}

void ExpressionBinder::validateNoNullLiteralChildren(const ParsedExpression& parsedExpression) {
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = parsedExpression.getChild(i);
        if (LITERAL_NULL == child->getExpressionType()) {
            throw invalid_argument("Expression " + parsedExpression.getRawName() +
                                   " cannot have null literal children.");
        }
    }
}

void ExpressionBinder::validateNumberOfChildren(
    const ParsedExpression& parsedExpression, uint32_t expectedNumChildren) {
    GF_ASSERT(parsedExpression.getExpressionType() == FUNCTION);
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    if (parsedExpression.getNumChildren() != expectedNumChildren) {
        throw invalid_argument("Expected " + to_string(expectedNumChildren) + " parameters for " +
                               parsedFunctionExpression.getFunctionName() + " function but get " +
                               to_string(parsedExpression.getNumChildren()) + ".");
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

void ExpressionBinder::validateExistsSubqueryHasNoAggregationOrOrderBy(
    const Expression& expression) {
    assert(expression.expressionType == EXISTENTIAL_SUBQUERY);
    auto subquery = ((ExistentialSubqueryExpression&)expression).getSubquery();
    vector<BoundProjectionBody*> projectionBodies;
    for (auto i = 0u; i < subquery->getNumQueryParts(); ++i) {
        projectionBodies.push_back(subquery->getQueryPart(i)->getProjectionBody());
    }
    for (auto& projectionBody : projectionBodies) {
        if (projectionBody->hasAggregationExpressions() ||
            projectionBody->hasOrderByExpressions()) {
            throw invalid_argument(
                "Expression " + expression.getRawName() +
                " is an existential subquery expression and should not contains any "
                "aggregation or order by in subquery RETURN or WITH clause.");
        }
    }
}

} // namespace binder
} // namespace graphflow
