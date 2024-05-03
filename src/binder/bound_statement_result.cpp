#include "binder/bound_statement_result.h"

#include "binder/expression/literal_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

BoundStatementResult BoundStatementResult::createSingleStringColumnResult(
    const std::string& columnName) {
    auto result = BoundStatementResult();
    auto value = Value(LogicalType::STRING(), columnName);
    auto stringColumn = std::make_shared<LiteralExpression>(std::move(value), columnName);
    result.addColumn(stringColumn);
    return result;
}

} // namespace binder
} // namespace kuzu
