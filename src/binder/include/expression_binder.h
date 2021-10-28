#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/parser/include/parsed_expression.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::parser;
using namespace graphflow::storage;

namespace graphflow {
namespace binder {

class QueryBinder;

class ExpressionBinder {

public:
    explicit ExpressionBinder(QueryBinder* queryBinder) : queryBinder{queryBinder} {}

    shared_ptr<Expression> bindExpression(const ParsedExpression& parsedExpression);

private:
    shared_ptr<Expression> bindBinaryBooleanExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindUnaryBooleanExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindComparisonExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindBinaryArithmeticExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindUnaryArithmeticExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindStringOperatorExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindCSVLineExtractExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindNullComparisonOperatorExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindPropertyExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindFunctionExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindAbsFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindFloorFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindCeilFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindCountStarFunctionExpression(
        const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindCountFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindAvgFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindSumMinMaxFunctionExpression(
        const ParsedExpression& parsedExpression, ExpressionType expressionType);
    shared_ptr<Expression> bindIDFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindDateFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindTimestampFunctionExpression(
        const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindIntervalFunctionExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindLiteralExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindVariableExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindExistentialSubqueryExpression(
        const ParsedExpression& parsedExpression);

    /****** validation *****/

    // NOTE: this validation should be removed and front end binds any null operation to null
    static void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression);
    // Parser cannot check expected number of children for built in functions. So number of children
    // validation is needed for function.
    static void validateNumberOfChildren(
        const ParsedExpression& parsedExpression, uint32_t expectedNumChildren);
    static void validateExpectedType(const Expression& expression, DataType expectedType);
    static void validateNumericalTypeOrUnstructured(const Expression& expression);
    static shared_ptr<Expression> validateAsBoolAndCastIfNecessary(
        shared_ptr<Expression> expression);
    // NOTE: this validation should be removed once we rewrite aggregation as an alias.
    static void validateAggregationIsRoot(const Expression& expression);
    static void validateTimestampArithmeticType(
        const ParsedExpression& parsedExpression, shared_ptr<Expression>& right);
    static void validateDateArithmeticType(
        const ParsedExpression& parsedExpression, shared_ptr<Expression>& right);
    static void validateIntervalArithmeticType(
        const ParsedExpression& parsedExpression, shared_ptr<Expression>& right);

private:
    template<typename T>
    shared_ptr<Expression> bindStringCastingFunctionExpression(
        const ParsedExpression& parsedExpression, ExpressionType castType,
        ExpressionType literalExpressionType, DataType resultDataType,
        std::function<T(const char*, uint64_t)> castFunction);

    QueryBinder* queryBinder;
};

} // namespace binder
} // namespace graphflow
