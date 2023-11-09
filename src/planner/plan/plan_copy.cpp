#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/not_implemented.h"
#include "planner/operator/logical_partitioner.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/scan/logical_index_scan.h"
#include "planner/planner.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

static void appendIndexScan(
    std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> infos, LogicalPlan& plan) {
    auto indexScan =
        std::make_shared<LogicalIndexScanNode>(std::move(infos), plan.getLastOperator());
    indexScan->computeFactorizedSchema();
    plan.setLastOperator(std::move(indexScan));
}

static void appendPartitioner(BoundCopyFromInfo* copyFromInfo, LogicalPlan& plan) {
    std::vector<std::unique_ptr<LogicalPartitionerInfo>> infos;
    auto readerConfig = reinterpret_cast<function::ScanBindData*>(
        copyFromInfo->fileScanInfo->copyFuncBindData.get())
                            ->config;
    auto fileType = readerConfig.fileType;
    // TODO(Xiyang): Merge TURTLE case with other data types.
    switch (fileType) {
    case FileType::TURTLE: {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRdfRelInfo*>(copyFromInfo->extraInfo.get());
        infos.push_back(std::make_unique<LogicalPartitionerInfo>(
            extraInfo->subjectOffset, copyFromInfo->columns, ColumnDataFormat::CSR));
        infos.push_back(std::make_unique<LogicalPartitionerInfo>(
            extraInfo->objectOffset, copyFromInfo->columns, ColumnDataFormat::CSR));
    } break;
    case FileType::CSV:
    case FileType::NPY:
    case FileType::PARQUET: {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
        auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
        // Partitioner for FWD direction rel data.
        infos.push_back(
            std::make_unique<LogicalPartitionerInfo>(extraInfo->srcOffset, copyFromInfo->columns,
                tableSchema->isSingleMultiplicityInDirection(RelDataDirection::FWD) ?
                    ColumnDataFormat::REGULAR :
                    ColumnDataFormat::CSR));
        // Partitioner for BWD direction rel data.
        infos.push_back(
            std::make_unique<LogicalPartitionerInfo>(extraInfo->dstOffset, copyFromInfo->columns,
                tableSchema->isSingleMultiplicityInDirection(RelDataDirection::BWD) ?
                    ColumnDataFormat::REGULAR :
                    ColumnDataFormat::CSR));
    } break;
        // LCOV_EXCL_START
    default: {
        throw NotImplementedException("PlanMapper::appendIndexScan");
    }
        // LCOV_EXCL_STOP
    }
    auto partitioner =
        std::make_shared<LogicalPartitioner>(std::move(infos), plan.getLastOperator());
    partitioner->computeFactorizedSchema();
    plan.setLastOperator(std::move(partitioner));
}

std::unique_ptr<LogicalPlan> Planner::planCopyFrom(const BoundStatement& statement) {
    auto& copyFrom = dynamic_cast<const BoundCopyFrom&>(statement);
    auto copyFromInfo = copyFrom.getInfo();
    auto readerConfig = reinterpret_cast<function::ScanBindData*>(
        copyFromInfo->fileScanInfo->copyFuncBindData.get())
                            ->config;
    auto fileType = readerConfig.fileType;
    auto plan = std::make_unique<LogicalPlan>();
    QueryPlanner::appendScanFile(copyFromInfo->fileScanInfo.get(), *plan);
    auto tableType = copyFromInfo->tableSchema->tableType;
    if (tableType == TableType::REL) {
        if (fileType != FileType::TURTLE) {
            auto extraInfo = dynamic_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
            std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> infos;
            auto srcNodeTableSchema = dynamic_cast<NodeTableSchema*>(extraInfo->srcTableSchema);
            auto srcTableID = srcNodeTableSchema->getTableID();
            auto srcPkType = srcNodeTableSchema->getPrimaryKey()->getDataType();
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(
                srcTableID, extraInfo->srcOffset, extraInfo->srcKey, srcPkType->copy()));
            auto dstNodeTableSchema = dynamic_cast<NodeTableSchema*>(extraInfo->dstTableSchema);
            auto dstTableID = dstNodeTableSchema->getTableID();
            auto dstPkType = dstNodeTableSchema->getPrimaryKey()->getDataType();
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(
                dstTableID, extraInfo->dstOffset, extraInfo->dstKey, dstPkType->copy()));
            appendIndexScan(std::move(infos), *plan);
        }
        appendPartitioner(copyFromInfo, *plan);
    }
    auto copy = make_shared<LogicalCopyFrom>(copyFromInfo->copy(),
        copyFrom.getStatementResult()->getSingleExpressionToCollect(), plan->getLastOperator());
    copy->computeFactorizedSchema();
    plan->setLastOperator(std::move(copy));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyTo(const Catalog& catalog,
    const NodesStoreStatsAndDeletedIDs& nodesStatistics, const RelsStoreStats& relsStatistics,
    const BoundStatement& statement) {
    auto& copyClause = reinterpret_cast<const BoundCopyTo&>(statement);
    auto regularQuery = copyClause.getRegularQuery();
    KU_ASSERT(regularQuery->getStatementType() == StatementType::QUERY);
    auto plan = QueryPlanner(catalog, nodesStatistics, relsStatistics).getBestPlan(*regularQuery);
    auto logicalCopyTo =
        make_shared<LogicalCopyTo>(plan->getLastOperator(), copyClause.getConfig()->copy());
    plan->setLastOperator(std::move(logicalCopyTo));
    return plan;
}

} // namespace planner
} // namespace kuzu
