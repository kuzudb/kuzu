#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/parser/expression/include/parsed_expression.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::parser;
using namespace graphflow::storage;

namespace graphflow {
namespace binder {

class QueryBinder;

class ExpressionBinder {
    friend class QueryBinder;

public:
    explicit ExpressionBinder(QueryBinder* queryBinder) : queryBinder{queryBinder} {}

    shared_ptr<Expression> bindExpression(const ParsedExpression& parsedExpression);

private:
    shared_ptr<Expression> bindBooleanExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindComparisonExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindBinaryArithmeticExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindBinaryDateArithmeticExpression(
        ExpressionType expressionType, shared_ptr<Expression> left, shared_ptr<Expression> right);
    shared_ptr<Expression> bindBinaryTimestampArithmeticExpression(
        ExpressionType expressionType, shared_ptr<Expression> left, shared_ptr<Expression> right);
    shared_ptr<Expression> bindBinaryIntervalArithmeticExpression(
        ExpressionType expressionType, shared_ptr<Expression> left, shared_ptr<Expression> right);

    shared_ptr<Expression> bindUnaryArithmeticExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindStringOperatorExpression(const ParsedExpression& parsedExpression);

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
    shared_ptr<Expression> bindListCreationFunction(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindLiteralExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindVariableExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindExistentialSubqueryExpression(
        const ParsedExpression& parsedExpression);

    /****** cast *****/
    static shared_ptr<Expression> castUnstructuredToBool(shared_ptr<Expression> expression);

    static shared_ptr<Expression> castExpressionToString(shared_ptr<Expression> expression);

    static shared_ptr<Expression> castToUnstructured(shared_ptr<Expression> expression);

    /****** validation *****/

    // NOTE: this validation should be removed and front end binds any null operation to null
    static void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression);

    // Parser cannot check expected number of children for built in functions. So number of children
    // validation is needed for function.
    static void validateNumberOfChildren(
        const ParsedExpression& parsedExpression, uint32_t expectedNumChildren);

    static void validateExpectedDataType(
        const Expression& expression, const unordered_set<DataType>& expectedTypes);

    static void validateNumeric(const Expression& expression) {
        validateExpectedDataType(expression, unordered_set<DataType>{INT64, DOUBLE});
    }
    static void validateNumericOrUnstructured(const Expression& expression) {
        validateExpectedDataType(expression, unordered_set<DataType>{INT64, DOUBLE, UNSTRUCTURED});
    }
    static void validateStringOrUnstructured(const Expression& expression) {
        validateExpectedDataType(expression, unordered_set<DataType>{STRING, UNSTRUCTURED});
    }

    static void validateExpectedBinaryOperation(const Expression& left, const Expression& right,
        ExpressionType type, const unordered_set<ExpressionType>& expectedTypes);

    // E.g. SUM(SUM(a.age)) is not allowed
    static void validateAggregationExpressionIsNotNested(const Expression& expression);

    static void validateExistsSubqueryHasNoAggregationOrOrderBy(const Expression& expression);

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
