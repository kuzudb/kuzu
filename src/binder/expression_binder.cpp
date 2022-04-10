#include "include/expression_binder.h"

#include "src/binder/expression/include/existential_subquery_expression.h"
#include "src/binder/expression/include/function_expression.h"
#include "src/binder/expression/include/literal_expression.h"
#include "src/binder/expression/include/parameter_expression.h"
#include "src/binder/expression/include/rel_expression.h"
#include "src/binder/include/query_binder.h"
#include "src/common/include/type_utils.h"
#include "src/function/boolean/include/vector_boolean_operations.h"
#include "src/function/cast/include/vector_cast_operations.h"
#include "src/function/comparison/include/vector_comparison_operations.h"
#include "src/function/null/include/vector_null_operations.h"
#include "src/function/string/include/vector_string_operations.h"
#include "src/parser/expression/include/parsed_function_expression.h"
#include "src/parser/expression/include/parsed_literal_expression.h"
#include "src/parser/expression/include/parsed_parameter_expression.h"
#include "src/parser/expression/include/parsed_property_expression.h"
#include "src/parser/expression/include/parsed_subquery_expression.h"
#include "src/parser/expression/include/parsed_variable_expression.h"

using namespace graphflow::function;

namespace graphflow {
namespace binder {

static bool containUnstructuredType(const expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (expression->dataType.typeID == UNSTRUCTURED) {
            return true;
        }
    }
    return false;
}

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    validateNoNullLiteralChildren(parsedExpression);
    shared_ptr<Expression> expression;
    auto expressionType = parsedExpression.getExpressionType();
    if (isExpressionBoolConnection(expressionType)) {
        expression = bindBooleanExpression(parsedExpression);
    } else if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
    } else if (isExpressionArithmetic(expressionType)) {
        expression = bindArithmeticExpression(parsedExpression);
    } else if (isExpressionStringOperator(expressionType)) {
        expression = bindStringOperatorExpression(parsedExpression);
    } else if (isExpressionNullOperator(expressionType)) {
        expression = bindNullOperatorExpression(parsedExpression);
    } else if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (PARAMETER == expressionType) {
        expression = bindParameterExpression(parsedExpression);
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
        // Boolean functions are non-overload functions. We cast input to BOOL.
        child = implicitCastIfNecessary(child, BOOL);
        children.push_back(move(child));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto execFunc = VectorBooleanOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorBooleanOperations::bindSelectFunction(expressionType, children);
    return make_shared<ScalarFunctionExpression>(
        expressionType, DataType(BOOL), move(children), move(execFunc), move(selectFunc));
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    assert(parsedExpression.getNumChildren() == 2);
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    children = castToUnstructuredIfNecessary(children);
    auto expressionType = parsedExpression.getExpressionType();
    auto execFunc = VectorComparisonOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorComparisonOperations::bindSelectFunction(expressionType, children);
    return make_shared<ScalarFunctionExpression>(
        expressionType, DataType(BOOL), move(children), move(execFunc), move(selectFunc));
}

shared_ptr<Expression> ExpressionBinder::bindArithmeticExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    vector<DataType> childrenTypes;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        childrenTypes.push_back(child->dataType);
        children.push_back(move(child));
    }
    auto builtInFunctions = queryBinder->catalog.getBuiltInFunctions();
    auto expressionType = parsedExpression.getExpressionType();
    auto functionName = expressionTypeToString(parsedExpression.getExpressionType());
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        childrenAfterCast.push_back(
            implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));
    }
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(function->returnTypeID),
        move(childrenAfterCast), function->execFunc);
}

shared_ptr<Expression> ExpressionBinder::bindStringOperatorExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        // String functions are non-overload functions. We cast input to STRING.
        child = implicitCastIfNecessary(child, STRING);
        children.push_back(move(child));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto execFunc = VectorStringOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorStringOperations::bindSelectFunction(expressionType, children);
    return make_shared<ScalarFunctionExpression>(
        expressionType, DataType(BOOL), move(children), move(execFunc), move(selectFunc));
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
        expressionType, DataType(BOOL), move(children), move(execFunc), move(selectFunc));
}

shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    auto& propertyExpression = (ParsedPropertyExpression&)parsedExpression;
    auto propertyName = propertyExpression.getPropertyName();
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, unordered_set<DataTypeID>{NODE, REL});
    auto& catalog = queryBinder->catalog;
    if (NODE == child->dataType.typeID) {
        auto node = static_pointer_cast<NodeExpression>(child);
        if (catalog.containNodeProperty(node->getLabel(), propertyName)) {
            auto& property = catalog.getNodeProperty(node->getLabel(), propertyName);
            return make_shared<PropertyExpression>(
                property.dataType, propertyName, property.id, move(child));
        } else {
            throw invalid_argument(
                "Node " + node->getRawName() + " does not have property " + propertyName + ".");
        }
    } else if (REL == child->dataType.typeID) {
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
    if (functionName == COUNT_STAR_FUNC_NAME) {
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
    } else {
        vector<DataType> childrenTypes;
        expression_vector children;
        for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
            auto child = bindExpression(*parsedExpression.getChild(i));
            childrenTypes.push_back(child->dataType);
            children.push_back(move(child));
        }
        auto builtInFunctions = queryBinder->catalog.getBuiltInFunctions();
        auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
        if (builtInFunctions->canApplyStaticEvaluation(functionName, children)) {
            return staticEvaluate(functionName, parsedExpression, children);
        }
        expression_vector childrenAfterCast;
        for (auto i = 0u; i < children.size(); ++i) {
            auto targetType = function->isVarLength ? function->parameterTypeIDs[0] :
                                                      function->parameterTypeIDs[i];
            childrenAfterCast.push_back(implicitCastIfNecessary(children[i], targetType));
        }
        auto returnType =
            function->returnTypeID == LIST ?
                DataType(LIST, make_unique<DataType>(childrenAfterCast[0]->dataType)) :
                DataType(function->returnTypeID);
        return make_shared<ScalarFunctionExpression>(
            FUNCTION, returnType, move(childrenAfterCast), function->execFunc);
    }
}

shared_ptr<Expression> ExpressionBinder::staticEvaluate(const string& functionName,
    const ParsedExpression& parsedExpression, const expression_vector& children) {
    if (functionName == CAST_TO_DATE_FUNCTION_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal.strVal;
        return make_shared<LiteralExpression>(LITERAL_DATE, DataType(DATE),
            Literal(Date::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_TIMESTAMP_FUNCTION_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal.strVal;
        return make_shared<LiteralExpression>(LITERAL_TIMESTAMP, DataType(TIMESTAMP),
            Literal(Timestamp::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_INTERVAL_FUNCTION_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal.strVal;
        return make_shared<LiteralExpression>(LITERAL_INTERVAL, DataType(INTERVAL),
            Literal(Interval::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == ID_FUNC_NAME) {
        return bindIDFunctionExpression(parsedExpression);
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindCountStarFunctionExpression(
    const ParsedExpression& parsedExpression) {
    validateNumberOfChildren(parsedExpression, 0);
    return make_shared<FunctionExpression>(COUNT_STAR_FUNC, DataType(INT64),
        queryBinder->getUniqueExpressionName(parsedExpression.getRawName()));
}

shared_ptr<Expression> ExpressionBinder::bindCountFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    return make_shared<FunctionExpression>(
        COUNT_FUNC, DataType(INT64), move(child), parsedFunctionExpression.getIsDistinct());
}

shared_ptr<Expression> ExpressionBinder::bindAvgFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    validateNumberOfChildren(parsedExpression, 1);
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateNumericOrUnstructured(*child);
    return make_shared<FunctionExpression>(
        AVG_FUNC, DataType(DOUBLE), move(child), parsedFunctionExpression.getIsDistinct());
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
    validateExpectedDataType(*child, unordered_set<DataTypeID>{NODE});
    return make_shared<PropertyExpression>(DataType(NODE_ID), INTERNAL_ID_SUFFIX,
        UINT32_MAX /* property key for internal id */, move(child));
}

shared_ptr<Expression> ExpressionBinder::bindParameterExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedParameterExpression = (ParsedParameterExpression&)parsedExpression;
    auto parameterName = parsedParameterExpression.getParameterName();
    if (parameterMap.contains(parameterName)) {
        return make_shared<ParameterExpression>(parameterName, parameterMap.at(parameterName));
    } else {
        auto literal = make_shared<Literal>();
        parameterMap.insert({parameterName, literal});
        return make_shared<ParameterExpression>(parameterName, literal);
    }
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

shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const shared_ptr<Expression>& expression, DataTypeID targetTypeID) {
    if (expression->dataType.typeID == targetTypeID) {
        return expression;
    }
    if (expression->dataType.typeID == INVALID) { // resolve type for parameter expression
        assert(expression->expressionType == PARAMETER);
        static_pointer_cast<ParameterExpression>(expression)->setDataType(DataType(targetTypeID));
        return expression;
    }
    switch (targetTypeID) {
    case BOOL: {
        return implicitCastToBool(expression);
    }
    case STRING: {
        return implicitCastToString(expression);
    }
    case UNSTRUCTURED: {
        return implicitCastToUnstructured(expression);
    }
    default:
        throw NotImplementedException("Expression " + expression->getRawName() + " has data type " +
                                      Types::dataTypeToString(expression->dataType) +
                                      " but expect " + Types::dataTypeToString(targetTypeID) +
                                      ". Implicit cast is not supported.");
    }
}

shared_ptr<Expression> ExpressionBinder::implicitCastToBool(
    const shared_ptr<Expression>& expression) {
    auto children = expression_vector{expression};
    auto execFunc = VectorCastOperations::bindImplicitCastToBool(children);
    return make_shared<ScalarFunctionExpression>(
        FUNCTION, DataType(BOOL), move(children), move(execFunc));
}

shared_ptr<Expression> ExpressionBinder::implicitCastToString(
    const shared_ptr<Expression>& expression) {
    auto children = expression_vector{expression};
    auto execFunc = VectorCastOperations::bindImplicitCastToString(children);
    return make_shared<ScalarFunctionExpression>(
        FUNCTION, DataType(STRING), move(children), move(execFunc));
}

shared_ptr<Expression> ExpressionBinder::implicitCastToUnstructured(
    const shared_ptr<Expression>& expression) {
    auto children = expression_vector{expression};
    auto execFunc = VectorCastOperations::bindImplicitCastToUnstructured(children);
    return make_shared<ScalarFunctionExpression>(
        FUNCTION, DataType(UNSTRUCTURED), move(children), move(execFunc));
}

expression_vector ExpressionBinder::castToUnstructuredIfNecessary(
    const expression_vector& parameters) {
    if (!containUnstructuredType(parameters)) {
        return parameters;
    }
    expression_vector result;
    for (auto& parameter : parameters) {
        result.push_back(implicitCastIfNecessary(parameter, UNSTRUCTURED));
    }
    return result;
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
    const Expression& expression, const unordered_set<DataTypeID>& expectedTypes) {
    auto dataType = expression.dataType;
    if (!expectedTypes.contains(dataType.typeID)) {
        string expectedTypesStr;
        vector<DataTypeID> expectedTypesVec{expectedTypes.begin(), expectedTypes.end()};
        auto numExpectedTypes = expectedTypes.size();
        for (auto i = 0u; i < numExpectedTypes - 1; ++i) {
            expectedTypesStr += Types::dataTypeToString(expectedTypesVec[i]) + ", ";
        }
        expectedTypesStr += Types::dataTypeToString(expectedTypesVec[numExpectedTypes - 1]);
        throw invalid_argument(expression.getRawName() + " has data type " +
                               Types::dataTypeToString(dataType.typeID) + ". " + expectedTypesStr +
                               " was expected.");
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
