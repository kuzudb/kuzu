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

    shared_ptr<Expression> bindArithmeticExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindStringOperatorExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindNullOperatorExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindPropertyExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindSpecialFunctions(const string& functionName,
        const ParsedExpression& parsedExpression, const expression_vector& children);
    shared_ptr<Expression> bindCountStarFunctionExpression(
        const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindCountFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindAvgFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindSumMinMaxFunctionExpression(
        const ParsedExpression& parsedExpression, ExpressionType expressionType);
    shared_ptr<Expression> bindIDFunctionExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindLiteralExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindVariableExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindExistentialSubqueryExpression(
        const ParsedExpression& parsedExpression);

    /****** cast *****/
    static shared_ptr<Expression> castUnstructuredToBool(shared_ptr<Expression> expression);

    // For overload functions (e.g. arithmetic and comparison), if any parameter is UNSTRUCTURED, we
    // cast all parameters as UNSTRUCTURED.
    static expression_vector castToUnstructuredIfNecessary(const expression_vector& parameters);

    template<typename T>
    static shared_ptr<Expression> castStringToTemporalLiteral(shared_ptr<Expression> expression,
        ExpressionType expressionType, DataType resultType,
        std::function<T(const char*, uint64_t)> castFunction);

    /****** validation *****/

    // NOTE: this validation should be removed and front end binds any null operation to null
    static void validateNoNullLiteralChildren(const ParsedExpression& parsedExpression);

    // Parser cannot check expected number of children for built in functions. So number of children
    // validation is needed for function.
    static void validateNumberOfChildren(
        const ParsedExpression& parsedExpression, uint32_t expectedNumChildren);

    static void validateExpectedDataType(
        const Expression& expression, const unordered_set<DataType>& expectedTypes);

    static void validateNumericOrUnstructured(const Expression& expression) {
        validateExpectedDataType(expression, unordered_set<DataType>{INT64, DOUBLE, UNSTRUCTURED});
    }

    // E.g. SUM(SUM(a.age)) is not allowed
    static void validateAggregationExpressionIsNotNested(const Expression& expression);

    static void validateExistsSubqueryHasNoAggregationOrOrderBy(const Expression& expression);

private:
    QueryBinder* queryBinder;
};

} // namespace binder
} // namespace graphflow
