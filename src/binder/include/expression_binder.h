#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/parser/include/parsed_expression.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::parser;
using namespace graphflow::binder;
using namespace graphflow::storage;

namespace graphflow {
namespace binder {

class ExpressionBinder {

public:
    explicit ExpressionBinder(const unordered_map<string, shared_ptr<Expression>>& variablesInScope,
        const Catalog& catalog)
        : variablesInScope{variablesInScope}, catalog{catalog} {}

    shared_ptr<Expression> bindExpression(const ParsedExpression& parsedExpression);

private:
    shared_ptr<Expression> bindBinaryBooleanExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindUnaryBooleanExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindComparisonExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindBinaryArithmeticExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindUnaryArithmeticExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindStringOperatorExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindNullComparisonOperatorExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindPropertyExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindFunctionExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindLiteralExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindVariableExpression(const ParsedExpression& parsedExpression);

private:
    const unordered_map<string, shared_ptr<Expression>>& variablesInScope;
    const Catalog& catalog;
};

} // namespace binder
} // namespace graphflow
