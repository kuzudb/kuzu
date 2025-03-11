#include "binder/bound_table_scan_info.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendTableFunctionCall(const BoundTableScanInfo& info, LogicalPlan& plan) {
    auto call = std::make_shared<LogicalTableFunctionCall>(info.func, info.bindData->copy());
    call->computeFactorizedSchema();
    plan.setLastOperator(std::move(call));
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(const BoundTableScanInfo& info) {
    return std::make_shared<LogicalTableFunctionCall>(info.func, info.bindData->copy());
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const BoundReadingClause& readingClause) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    return std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(),
        call.getBindData()->copy());
}

} // namespace planner
} // namespace kuzu
