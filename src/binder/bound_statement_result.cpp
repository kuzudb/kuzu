#include "binder/bound_statement_result.h"

#include "binder/expression/literal_expression.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatementResult> BoundStatementResult::createSingleStringColumnResult() {
    auto result = std::make_unique<BoundStatementResult>();
    auto columnName = std::string("result");
    auto value = std::make_unique<common::Value>(
        common::LogicalType{common::LogicalTypeID::STRING}, columnName);
    auto stringColumn = std::make_shared<LiteralExpression>(std::move(value), columnName);
    result->addColumn(stringColumn, expression_vector{stringColumn});
    return result;
}

expression_vector BoundStatementResult::getExpressionsToCollect() {
    expression_vector result;
    for (auto& expressionsToCollect : expressionsToCollectPerColumn) {
        for (auto& expression : expressionsToCollect) {
            result.push_back(expression);
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
