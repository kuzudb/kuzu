#include "include/expression_binder.h"

#include "src/binder/expression/include/existential_subquery_expression.h"
#include "src/binder/expression/include/function_expression.h"
#include "src/binder/expression/include/literal_expression.h"
#include "src/binder/expression/include/parameter_expression.h"
#include "src/binder/expression/include/rel_expression.h"
#include "src/binder/include/binder.h"
#include "src/common/include/type_utils.h"
#include "src/function/boolean/include/vector_boolean_operations.h"
#include "src/function/null/include/vector_null_operations.h"
#include "src/parser/expression/include/parsed_function_expression.h"
#include "src/parser/expression/include/parsed_literal_expression.h"
#include "src/parser/expression/include/parsed_parameter_expression.h"
#include "src/parser/expression/include/parsed_property_expression.h"
#include "src/parser/expression/include/parsed_subquery_expression.h"
#include "src/parser/expression/include/parsed_variable_expression.h"

using namespace kuzu::function;

namespace kuzu {
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
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    return bindBooleanExpression(parsedExpression.getExpressionType(), children);
}

shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
    ExpressionType expressionType, const expression_vector& children) {
    expression_vector childrenAfterCast;
    for (auto& child : children) {
        childrenAfterCast.push_back(implicitCastIfNecessary(child, BOOL));
    }
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc = VectorBooleanOperations::bindExecFunction(expressionType, childrenAfterCast);
    auto selectFunc =
        VectorBooleanOperations::bindSelectFunction(expressionType, childrenAfterCast);
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(functionName, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(BOOL),
        move(childrenAfterCast), move(execFunc), move(selectFunc), uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        children.push_back(move(child));
    }
    return bindComparisonExpression(parsedExpression.getExpressionType(), std::move(children));
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    ExpressionType expressionType, const expression_vector& children) {
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    auto functionName = expressionTypeToString(expressionType);
    vector<DataType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }
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
    if (NODE == child->dataType.typeID) {
        return bindNodePropertyExpression(child, propertyName);
    } else if (REL == child->dataType.typeID) {
        return bindRelPropertyExpression(child, propertyName);
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindNodePropertyExpression(
    shared_ptr<Expression> node, const string& propertyName) {
    auto catalogContent = binder->catalog.getReadOnlyVersion();
    auto nodeExpression = static_pointer_cast<NodeExpression>(node);
    if (nodeExpression->getNumTableIDs() > 1) {
        throw BinderException("Cannot bind property for multi-labeled node " + node->getRawName());
    }
    if (catalogContent->containNodeProperty(nodeExpression->getTableID(), propertyName)) {
        auto& property =
            catalogContent->getNodeProperty(nodeExpression->getTableID(), propertyName);
        return bindNodePropertyExpression(node, property);
    } else {
        throw BinderException("Node " + nodeExpression->getRawName() + " does not have property " +
                              propertyName + ".");
    }
}

shared_ptr<Expression> ExpressionBinder::bindNodePropertyExpression(
    shared_ptr<Expression> node, const Property& property) {
    return make_shared<PropertyExpression>(
        property.dataType, property.name, property.propertyID, move(node));
}

shared_ptr<Expression> ExpressionBinder::bindRelPropertyExpression(
    shared_ptr<Expression> rel, const string& propertyName) {
    auto catalogContent = binder->catalog.getReadOnlyVersion();
    auto relExpression = static_pointer_cast<RelExpression>(rel);
    if (catalogContent->containRelProperty(relExpression->getTableID(), propertyName)) {
        auto& property = catalogContent->getRelProperty(relExpression->getTableID(), propertyName);
        if (TableSchema::isReservedPropertyName(propertyName)) {
            throw BinderException(
                propertyName + " is reserved for system usage. External access is not allowed.");
        }
        return bindRelPropertyExpression(rel, property);
    } else {
        throw BinderException(
            "Rel " + rel->getRawName() + " does not have property " + propertyName + ".");
    }
}

shared_ptr<Expression> ExpressionBinder::bindRelPropertyExpression(
    shared_ptr<Expression> rel, const Property& property) {
    auto relExpression = static_pointer_cast<RelExpression>(rel);
    if (relExpression->isVariableLength()) {
        throw BinderException(
            "Cannot read property of variable length rel " + rel->getRawName() + ".");
    }
    return make_shared<PropertyExpression>(
        property.dataType, property.name, property.propertyID, std::move(rel));
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
        // e.g. COUNT(a) -> COUNT(a._id)
        if (child->dataType.typeID == NODE || child->dataType.typeID == REL) {
            child = bindInternalIDExpression(child);
        }
        childrenTypes.push_back(child->dataType);
        children.push_back(std::move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes, isDistinct);
    auto uniqueExpressionName =
        AggregateFunctionExpression::getUniqueName(function->name, children, function->isDistinct);
    if (children.empty()) {
        uniqueExpressionName = binder->getUniqueExpressionName(uniqueExpressionName);
    }
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
        return bindInternalIDExpression(parsedExpression);
    }
    assert(false);
}

shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, unordered_set<DataTypeID>{NODE, REL});
    return bindInternalIDExpression(child);
}

shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(
    shared_ptr<Expression> nodeOrRel) {
    if (nodeOrRel->dataType.typeID == NODE) {
        return ((NodeExpression*)nodeOrRel.get())->getNodeIDPropertyExpression();
    } else if (nodeOrRel->dataType.typeID == REL) {
        auto rel = (RelExpression*)nodeOrRel.get();
        auto relTableSchema =
            binder->catalog.getReadOnlyVersion()->getRelTableSchema(rel->getTableID());
        return bindRelPropertyExpression(nodeOrRel, relTableSchema->getRelIDDefinition());
    }
    assert(false);
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
    if (literal->isNull()) {
        return LiteralExpression::createNullLiteralExpression(
            binder->getUniqueExpressionName("NULL"));
    }
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
    auto [queryGraph, _] = binder->bindGraphPattern(subqueryExpression.getPatternElements());
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
    const shared_ptr<Expression>& expression, DataType targetType) {
    if (targetType.typeID == ANY || expression->dataType == targetType) {
        return expression;
    }
    if (expression->dataType.typeID == ANY) {
        resolveAnyDataType(*expression, targetType);
        return expression;
    }
    return implicitCast(expression, targetType);
}

shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const shared_ptr<Expression>& expression, DataTypeID targetTypeID) {
    if (targetTypeID == ANY || expression->dataType.typeID == targetTypeID) {
        return expression;
    }
    if (expression->dataType.typeID == ANY) {
        if (targetTypeID == LIST) {
            // e.g. len($1) we cannot infer the child type for $1.
            throw BinderException("Cannot resolve recursive data type for expression " +
                                  expression->getRawName() + ".");
        }
        resolveAnyDataType(*expression, DataType(targetTypeID));
        return expression;
    }
    assert(targetTypeID != LIST);
    return implicitCast(expression, DataType(targetTypeID));
}

void ExpressionBinder::resolveAnyDataType(Expression& expression, DataType targetType) {
    if (expression.expressionType == PARAMETER) { // expression is parameter
        ((ParameterExpression&)expression).setDataType(targetType);
    } else { // expression is null literal
        assert(expression.expressionType == LITERAL);
        ((LiteralExpression&)expression).setDataType(targetType);
    }
}

shared_ptr<Expression> ExpressionBinder::implicitCast(
    const shared_ptr<Expression>& expression, DataType targetType) {
    throw BinderException("Expression " + expression->getRawName() + " has data type " +
                          Types::dataTypeToString(expression->dataType) + " but expect " +
                          Types::dataTypeToString(targetType) +
                          ". Implicit cast is not supported.");
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
} // namespace kuzu
