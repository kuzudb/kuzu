#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/catalog/include/catalog.h"
#include "src/common/types/include/literal.h"
#include "src/parser/expression/include/parsed_expression.h"

using namespace graphflow::common;
using namespace graphflow::parser;
using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class Binder;

class ExpressionBinder {
    friend class Binder;

public:
    explicit ExpressionBinder(Binder* queryBinder) : binder{queryBinder} {}

    shared_ptr<Expression> bindExpression(const ParsedExpression& parsedExpression);

private:
    shared_ptr<Expression> bindBooleanExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindBooleanExpression(
        ExpressionType expressionType, const expression_vector& children);

    shared_ptr<Expression> bindComparisonExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindComparisonExpression(
        ExpressionType expressionType, const expression_vector& children);

    shared_ptr<Expression> bindNullOperatorExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindPropertyExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindNodePropertyExpression(
        shared_ptr<Expression> node, const string& propertyName);
    shared_ptr<Expression> bindNodePropertyExpression(
        shared_ptr<Expression> node, const Property& property);
    shared_ptr<Expression> bindRelPropertyExpression(
        shared_ptr<Expression> rel, const string& propertyName);
    shared_ptr<Expression> bindRelPropertyExpression(
        shared_ptr<Expression> rel, const Property& property);

    shared_ptr<Expression> bindFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindScalarFunctionExpression(
        const ParsedExpression& parsedExpression, const string& functionName);
    shared_ptr<Expression> bindAggregateFunctionExpression(
        const ParsedExpression& parsedExpression, const string& functionName, bool isDistinct);

    shared_ptr<Expression> staticEvaluate(const string& functionName,
        const ParsedExpression& parsedExpression, const expression_vector& children);

    shared_ptr<Expression> bindInternalIDExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindInternalIDExpression(shared_ptr<Expression> nodeOrRel);

    shared_ptr<Expression> bindParameterExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindLiteralExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindVariableExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindExistentialSubqueryExpression(
        const ParsedExpression& parsedExpression);

    /****** cast *****/
    static shared_ptr<Expression> implicitCastIfNecessary(
        const shared_ptr<Expression>& expression, DataTypeID targetTypeID);
    static shared_ptr<Expression> implicitCastToBool(const shared_ptr<Expression>& expression);
    static shared_ptr<Expression> implicitCastToInt64(const shared_ptr<Expression>& expression);
    static shared_ptr<Expression> implicitCastToUnstructured(
        const shared_ptr<Expression>& expression);
    static shared_ptr<Expression> implicitCastToString(const shared_ptr<Expression>& expression);
    static shared_ptr<Expression> implicitCastToTimestamp(const shared_ptr<Expression>& expression);

    /****** validation *****/
    static void validateExpectedDataType(const Expression& expression, DataTypeID target) {
        validateExpectedDataType(expression, unordered_set<DataTypeID>{target});
    }
    static void validateExpectedDataType(
        const Expression& expression, const unordered_set<DataTypeID>& targets);
    // E.g. SUM(SUM(a.age)) is not allowed
    static void validateAggregationExpressionIsNotNested(const Expression& expression);

private:
    Binder* binder;
    unordered_map<string, shared_ptr<Literal>> parameterMap;
};

} // namespace binder
} // namespace graphflow
