#include "binder/query/reading_clause/bound_table_function_call.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const BoundReadingClause& readingClause) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    return std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(),
        call.getBindData()->copy(), call.getOutExprs(), call.getRowIdxExpr());
}

} // namespace planner
} // namespace kuzu
