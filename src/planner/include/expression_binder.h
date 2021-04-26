#pragma once

#include <unordered_set>

#include "src/expression/include/logical/logical_expression.h"
#include "src/parser/include/parsed_expression.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::parser;
using namespace graphflow::expression;
using namespace graphflow::storage;

namespace graphflow {
namespace planner {

class ExpressionBinder {

public:
    explicit ExpressionBinder(
        const unordered_map<string, shared_ptr<LogicalExpression>>& variablesInScope,
        const Catalog& catalog)
        : variablesInScope{variablesInScope}, catalog{catalog} {}

    shared_ptr<LogicalExpression> bindExpression(const ParsedExpression& parsedExpression);

private:
    shared_ptr<LogicalExpression> bindBinaryBooleanExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindUnaryBooleanExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindComparisonExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindBinaryArithmeticExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindUnaryArithmeticExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindStringOperatorExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindNullComparisonOperatorExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindPropertyExpression(const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindFunctionExpression(const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindLiteralExpression(const ParsedExpression& parsedExpression);

    shared_ptr<LogicalExpression> bindVariableExpression(const ParsedExpression& parsedExpression);

private:
    const unordered_map<string, shared_ptr<LogicalExpression>>& variablesInScope;
    const Catalog& catalog;
};

} // namespace planner
} // namespace graphflow
