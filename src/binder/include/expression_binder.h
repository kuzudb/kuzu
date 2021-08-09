#pragma once

#include "src/binder/include/binder_context.h"
#include "src/parser/include/parsed_expression.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::parser;
using namespace graphflow::storage;

namespace graphflow {
namespace binder {

class ExpressionBinder {

public:
    ExpressionBinder(const Catalog& catalog, const BinderContext& context)
        : catalog{catalog}, context{context} {}

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
    shared_ptr<Expression> bindCountStarFunctionExpression(
        const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindIDFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindDateFunctionExpression(const ParsedExpression& parsedExpression);

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
    static void validateNumericalType(const Expression& expression);
    static shared_ptr<Expression> validateAsBoolAndCastIfNecessary(
        shared_ptr<Expression> expression);

private:
    const Catalog& catalog;
    const BinderContext& context;
};

} // namespace binder
} // namespace graphflow
