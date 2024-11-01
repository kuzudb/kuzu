#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "planner/operator/logical_partitioner.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/scan/logical_index_look_up.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace planner {

static void appendIndexScan(const ExtraBoundCopyRelInfo& extraInfo, LogicalPlan& plan) {
    auto indexScan =
        std::make_shared<LogicalPrimaryKeyLookup>(extraInfo.infos, plan.getLastOperator());
    indexScan->computeFactorizedSchema();
    plan.setLastOperator(std::move(indexScan));
}

static void appendPartitioner(const BoundCopyFromInfo& copyFromInfo, LogicalPlan& plan) {
    LogicalPartitionerInfo info(copyFromInfo.tableEntry, copyFromInfo.offset);
    // Partitioner for FWD direction rel data.
    info.partitioningInfos.push_back(LogicalPartitioningInfo(RelKeyIdx::FWD /* keyIdx */));
    // Partitioner for BWD direction rel data.
    info.partitioningInfos.push_back(LogicalPartitioningInfo(RelKeyIdx::BWD /* keyIdx */));
    auto partitioner = std::make_shared<LogicalPartitioner>(std::move(info), copyFromInfo.copy(),
        plan.getLastOperator());
    partitioner->computeFactorizedSchema();
    plan.setLastOperator(std::move(partitioner));
}

static void appendCopyFrom(const BoundCopyFromInfo& info, expression_vector outExprs,
    LogicalPlan& plan) {
    auto op =
        make_shared<LogicalCopyFrom>(info.copy(), std::move(outExprs), plan.getLastOperator());
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
}

std::unique_ptr<LogicalPlan> Planner::planCopyFrom(const BoundStatement& statement) {
    auto& copyFrom = statement.constCast<BoundCopyFrom>();
    auto outExprs = statement.getStatementResult()->getColumns();
    auto copyFromInfo = copyFrom.getInfo();
    auto tableType = copyFromInfo->tableEntry->getTableType();
    switch (tableType) {
    case TableType::NODE: {
        return planCopyNodeFrom(copyFromInfo, outExprs);
    }
    case TableType::REL: {
        return planCopyRelFrom(copyFromInfo, outExprs);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<LogicalPlan> Planner::planCopyNodeFrom(const BoundCopyFromInfo* info,
    expression_vector results) {
    auto plan = std::make_unique<LogicalPlan>();
    switch (info->source->type) {
    case ScanSourceType::FILE:
    case ScanSourceType::OBJECT: {
        auto& scanSource = info->source->constCast<BoundTableScanSource>();
        appendTableFunctionCall(scanSource.info, *plan);
    } break;
    case ScanSourceType::QUERY: {
        auto& querySource = info->source->constCast<BoundQueryScanSource>();
        plan = getBestPlan(planQuery(*querySource.statement));
        appendAccumulate(AccumulateType::REGULAR, plan->getSchema()->getExpressionsInScope(),
            info->offset, nullptr /* mark */, *plan);
    } break;
    default:
        KU_UNREACHABLE;
    }
    appendCopyFrom(*info, results, *plan);
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyResourceFrom(const BoundCopyFromInfo* info,
    expression_vector results) {
    auto plan = std::make_unique<LogicalPlan>();
    KU_ASSERT(info->source->type == ScanSourceType::FILE);
    auto& scanSource = info->source->constCast<BoundTableScanSource>();
    appendTableFunctionCall(scanSource.info, *plan);
    appendDistinct(scanSource.info.columns, *plan);
    appendCopyFrom(*info, results, *plan);
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyRelFrom(const BoundCopyFromInfo* info,
    expression_vector results) {
    auto plan = std::make_unique<LogicalPlan>();
    switch (info->source->type) {
    case ScanSourceType::FILE:
    case ScanSourceType::OBJECT: {
        auto& fileSource = info->source->constCast<BoundTableScanSource>();
        appendTableFunctionCall(fileSource.info, info->offset, *plan);
    } break;
    case ScanSourceType::QUERY: {
        auto& querySource = info->source->constCast<BoundQueryScanSource>();
        plan = getBestPlan(planQuery(*querySource.statement));
        appendAccumulate(AccumulateType::REGULAR, plan->getSchema()->getExpressionsInScope(),
            info->offset, nullptr /* mark */, *plan);
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto& extraInfo = info->extraInfo->constCast<ExtraBoundCopyRelInfo>();
    appendIndexScan(extraInfo, *plan);
    appendPartitioner(*info, *plan);
    appendCopyFrom(*info, results, *plan);
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyTo(const BoundStatement& statement) {
    auto& boundCopyTo = statement.constCast<BoundCopyTo>();
    auto regularQuery = boundCopyTo.getRegularQuery();
    std::vector<std::string> columnNames;
    for (auto& column : regularQuery->getStatementResult()->getColumns()) {
        columnNames.push_back(column->toString());
    }
    KU_ASSERT(regularQuery->getStatementType() == StatementType::QUERY);
    auto plan = getBestPlan(*regularQuery);
    auto copyTo = make_shared<LogicalCopyTo>(boundCopyTo.getBindData()->copy(),
        boundCopyTo.getExportFunc(), plan->getLastOperator());
    plan->setLastOperator(std::move(copyTo));
    return plan;
}

} // namespace planner
} // namespace kuzu
