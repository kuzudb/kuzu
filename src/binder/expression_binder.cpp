#include "include/expression_binder.h"

#include "src/binder/expression/include/existential_subquery_expression.h"
#include "src/binder/expression/include/function_expression.h"
#include "src/binder/expression/include/literal_expression.h"
#include "src/binder/expression/include/parameter_expression.h"
#include "src/binder/expression/include/rel_expression.h"
#include "src/binder/include/binder.h"
#include "src/common/include/type_utils.h"
#include "src/function/boolean/include/vector_boolean_operations.h"
#include "src/function/cast/include/vector_cast_operations.h"
#include "src/function/null/include/vector_null_operations.h"
#include "src/parser/expression/include/parsed_function_expression.h"
#include "src/parser/expression/include/parsed_literal_expression.h"
#include "src/parser/expression/include/parsed_parameter_expression.h"
#include "src/parser/expression/include/parsed_property_expression.h"
#include "src/parser/expression/include/parsed_subquery_expression.h"
#include "src/parser/expression/include/parsed_variable_expression.h"

using namespace graphflow::function;

namespace graphflow {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    shared_ptr<Expression> expression;
    auto expressionType = parsedExpression.getExpressionType();
    if (isExpressionBoolConnection(expressionType)) {
        expression = bindBooleanExpression(parsedExpression);
    } else if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
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
        assert(false);
    }
    if (parsedExpression.hasAlias()) {
        expression->setAlias(parsedExpression.getAlias());
    }
    expression->setRawName(parsedExpression.getRawName());
    if (isExpressionAggregate(expression->expressionType)) {
        validateAggregationExpressionIsNotNested(*expression);
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
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc = VectorBooleanOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorBooleanOperations::bindSelectFunction(expressionType, children);
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(functionName, children);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(BOOL), move(children),
        move(execFunc), move(selectFunc), uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    vector<DataType> childrenTypes;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        childrenTypes.push_back(child->dataType);
        children.push_back(move(child));
    }
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    auto expressionType = parsedExpression.getExpressionType();
    auto functionName = expressionTypeToString(parsedExpression.getExpressionType());
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        childrenAfterCast.push_back(
            implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(function->returnTypeID),
        move(childrenAfterCast), function->execFunc, function->selectFunc, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindNullOperatorExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc = VectorNullOperations::bindExecFunction(expressionType, children);
    auto selectFunc = VectorNullOperations::bindSelectFunction(expressionType, children);
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(functionName, children);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(BOOL), move(children),
        move(execFunc), move(selectFunc), uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    auto& propertyExpression = (ParsedPropertyExpression&)parsedExpression;
    auto propertyName = propertyExpression.getPropertyName();
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, unordered_set<DataTypeID>{NODE, REL});
    auto& catalog = binder->catalog;
    if (NODE == child->dataType.typeID) {
        return bindNodePropertyExpression(child, propertyName);
    } else if (REL == child->dataType.typeID) {
        auto rel = static_pointer_cast<RelExpression>(child);
        if (catalog.getReadOnlyVersion()->containRelProperty(rel->getTableID(), propertyName)) {
            auto& property =
                catalog.getReadOnlyVersion()->getRelProperty(rel->getTableID(), propertyName);
            return make_shared<PropertyExpression>(
                property.dataType, propertyName, property.propertyID, move(child));
        } else {
            throw BinderException(
                "Rel " + rel->getRawName() + " does not have property " + propertyName + ".");
        }
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindNodePropertyExpression(
    shared_ptr<Expression> node, const string& propertyName) {
    auto& catalog = binder->catalog;
    auto nodeExpression = static_pointer_cast<NodeExpression>(node);
    if (catalog.getReadOnlyVersion()->containNodeProperty(
            nodeExpression->getTableID(), propertyName)) {
        auto& property = catalog.getReadOnlyVersion()->getNodeProperty(
            nodeExpression->getTableID(), propertyName);
        return make_shared<PropertyExpression>(
            property.dataType, propertyName, property.propertyID, move(node));
    } else {
        throw BinderException("Node " + nodeExpression->getRawName() + " does not have property " +
                              propertyName + ".");
    }
}

shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    auto functionName = parsedFunctionExpression.getFunctionName();
    StringUtils::toUpper(functionName);
    auto functionType = binder->catalog.getFunctionType(functionName);
    if (functionType == FUNCTION) {
        return bindScalarFunctionExpression(parsedExpression, functionName);
    } else {
        assert(functionType == AGGREGATE_FUNCTION);
        return bindAggregateFunctionExpression(
            parsedExpression, functionName, parsedFunctionExpression.getIsDistinct());
    }
}

shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const ParsedExpression& parsedExpression, const string& functionName) {
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    vector<DataType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        childrenTypes.push_back(child->dataType);
        children.push_back(move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    if (builtInFunctions->canApplyStaticEvaluation(functionName, children)) {
        return staticEvaluate(functionName, parsedExpression, children);
    }
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        auto targetType =
            function->isVarLength ? function->parameterTypeIDs[0] : function->parameterTypeIDs[i];
        childrenAfterCast.push_back(implicitCastIfNecessary(children[i], targetType));
    }
    DataType returnType;
    if (function->bindFunc) {
        function->bindFunc(childrenTypes, function, returnType);
    } else {
        returnType = DataType(function->returnTypeID);
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(FUNCTION, returnType, move(childrenAfterCast),
        function->execFunc, function->selectFunc, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const string& functionName, bool isDistinct) {
    auto builtInFunctions = binder->catalog.getBuiltInAggregateFunction();
    vector<DataType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        // rewrite aggregate on node or rel as aggregate on their internal IDs.
        if (child->dataType.typeID == NODE) {
            auto node = static_pointer_cast<NodeExpression>(child);
            child = node->getNodeIDPropertyExpression();
        } else if (child->dataType.typeID == REL) {
            auto rel = static_pointer_cast<RelExpression>(child);
            auto idProperty = binder->catalog.getReadOnlyVersion()->getRelProperty(
                rel->getTableID(), INTERNAL_ID_SUFFIX);
            child = make_shared<PropertyExpression>(
                idProperty.dataType, INTERNAL_ID_SUFFIX, idProperty.propertyID, std::move(child));
        }
        childrenTypes.push_back(child->dataType);
        children.push_back(std::move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes, isDistinct);
    auto uniqueExpressionName =
        AggregateFunctionExpression::getUniqueName(function->name, children, function->isDistinct);
    return make_shared<AggregateFunctionExpression>(DataType(function->returnTypeID),
        move(children), function->aggregateFunction->clone(), uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::staticEvaluate(const string& functionName,
    const ParsedExpression& parsedExpression, const expression_vector& children) {
    if (functionName == CAST_TO_DATE_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(DATE),
            make_unique<Literal>(Date::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_TIMESTAMP_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(TIMESTAMP),
            make_unique<Literal>(Timestamp::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_INTERVAL_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(INTERVAL),
            make_unique<Literal>(Interval::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == ID_FUNC_NAME) {
        return bindIDFunctionExpression(parsedExpression);
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindIDFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, NODE);
    return ((NodeExpression&)*child).getNodeIDPropertyExpression();
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
    auto literal = literalExpression.getLiteral();
    return make_shared<LiteralExpression>(literal->dataType, make_unique<Literal>(*literal));
}

shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto& variableExpression = (ParsedVariableExpression&)parsedExpression;
    auto variableName = variableExpression.getVariableName();
    if (binder->variablesInScope.contains(variableName)) {
        return binder->variablesInScope.at(variableName);
    }
    throw BinderException("Variable " + parsedExpression.getRawName() + " is not in scope.");
}

shared_ptr<Expression> ExpressionBinder::bindExistentialSubqueryExpression(
    const ParsedExpression& parsedExpression) {
    auto& subqueryExpression = (ParsedSubqueryExpression&)parsedExpression;
    auto prevVariablesInScope = binder->enterSubquery();
    auto queryGraph = binder->bindQueryGraph(subqueryExpression.getPatternElements());
    auto name = binder->getUniqueExpressionName(parsedExpression.getRawName());
    auto boundSubqueryExpression =
        make_shared<ExistentialSubqueryExpression>(std::move(queryGraph), std::move(name));
    if (subqueryExpression.hasWhereClause()) {
        boundSubqueryExpression->setWhereExpression(
            binder->bindWhereExpression(*subqueryExpression.getWhereClause()));
    }
    binder->exitSubquery(move(prevVariablesInScope));
    return boundSubqueryExpression;
}

shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const shared_ptr<Expression>& expression, DataTypeID targetTypeID) {
    if (expression->dataType.typeID == targetTypeID || targetTypeID == ANY) {
        return expression;
    }
    if (expression->dataType.typeID == ANY) { // resolve type for parameter expression
        auto isParameterExpression = expression->expressionType == PARAMETER;
        auto isNullLiteralExpression =
            expression->expressionType == LITERAL && ((LiteralExpression&)*expression).isNull();
        assert(isParameterExpression || isNullLiteralExpression);
        static_pointer_cast<ParameterExpression>(expression)->setDataType(DataType(targetTypeID));
        return expression;
    }
    switch (targetTypeID) {
    case BOOL: {
        return implicitCastToBool(expression);
    }
    case INT64: {
        return implicitCastToInt64(expression);
    }
    case STRING: {
        return implicitCastToString(expression);
    }
    case TIMESTAMP: {
        return implicitCastToTimestamp(expression);
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
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(IMPLICIT_CAST_TO_BOOL_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(FUNCTION, DataType(BOOL), move(children),
        move(execFunc), nullptr /* selectFunc */, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::implicitCastToInt64(
    const shared_ptr<Expression>& expression) {
    auto children = expression_vector{expression};
    auto execFunc = VectorCastOperations::bindImplicitCastToInt64(children);
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(IMPLICIT_CAST_TO_INT_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(FUNCTION, DataType(INT64), move(children),
        move(execFunc), nullptr /* selectFunc */, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::implicitCastToString(
    const shared_ptr<Expression>& expression) {
    auto children = expression_vector{expression};
    auto execFunc = VectorCastOperations::bindImplicitCastToString(children);
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(IMPLICIT_CAST_TO_STRING_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(FUNCTION, DataType(STRING), move(children),
        move(execFunc), nullptr /* selectFunc */, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::implicitCastToTimestamp(
    const shared_ptr<Expression>& expression) {
    auto children = expression_vector{expression};
    auto execFunc = VectorCastOperations::bindImplicitCastToTimestamp(children);
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(IMPLICIT_CAST_TO_TIMESTAMP_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(FUNCTION, DataType(TIMESTAMP), move(children),
        move(execFunc), nullptr /* selectFunc */, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::implicitCastToUnstructured(
    const shared_ptr<Expression>& expression) {
    auto children = expression_vector{expression};
    auto execFunc = VectorCastOperations::bindImplicitCastToUnstructured(children);
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(IMPLICIT_CAST_TO_UNSTRUCTURED_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(FUNCTION, DataType(UNSTRUCTURED), move(children),
        move(execFunc), nullptr /* selectFunc */, uniqueExpressionName);
}

void ExpressionBinder::validateExpectedDataType(
    const Expression& expression, const unordered_set<DataTypeID>& targets) {
    auto dataType = expression.dataType;
    if (!targets.contains(dataType.typeID)) {
        vector<DataTypeID> targetsVec{targets.begin(), targets.end()};
        throw BinderException(expression.getRawName() + " has data type " +
                              Types::dataTypeToString(dataType.typeID) + ". " +
                              Types::dataTypesToString(targetsVec) + " was expected.");
    }
}

void ExpressionBinder::validateAggregationExpressionIsNotNested(const Expression& expression) {
    if (expression.getNumChildren() == 0) {
        return;
    }
    if (expression.getChild(0)->hasAggregationExpression()) {
        throw BinderException(
            "Expression " + expression.getRawName() + " contains nested aggregation.");
    }
}

} // namespace binder
} // namespace graphflow
