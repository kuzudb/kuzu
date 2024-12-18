#include "binder/copy/bound_table_scan_info.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendTableFunctionCall(const BoundTableScanSourceInfo& info, LogicalPlan& plan) {
    auto call =
        std::make_shared<LogicalTableFunctionCall>(info.func, info.bindData->copy(), info.columns);
    call->computeFactorizedSchema();
    plan.setLastOperator(std::move(call));
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const binder::BoundTableScanSourceInfo& info) {
    return std::make_shared<LogicalTableFunctionCall>(info.func, info.bindData->copy(),
        info.columns);
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const BoundReadingClause& readingClause) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    return std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(),
        call.getBindData()->copy(), call.getColumns());
}

} // namespace planner
} // namespace kuzu
