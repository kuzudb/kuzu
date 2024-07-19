#include "binder/copy/bound_table_scan_info.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendTableFunctionCall(const BoundTableScanSourceInfo& info, LogicalPlan& plan) {
    appendTableFunctionCall(info, nullptr /* nullptr */, plan);
}

void Planner::appendTableFunctionCall(const BoundTableScanSourceInfo& info,
    std::shared_ptr<Expression> offset, LogicalPlan& plan) {
    auto call = std::make_shared<LogicalTableFunctionCall>(info.func, info.bindData->copy(),
        info.columns, offset);
    call->computeFactorizedSchema();
    plan.setLastOperator(std::move(call));
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const binder::BoundTableScanSourceInfo& info) {
    return std::make_shared<LogicalTableFunctionCall>(info.func, info.bindData->copy(),
        info.columns, nullptr /* offset */);
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const BoundReadingClause& readingClause) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    return std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(),
        call.getBindData()->copy(), call.getColumns(), call.getOffset());
}

} // namespace planner
} // namespace kuzu
