#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "binder/expression/variable_expression.h"
#include "catalog/node_table_schema.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/scan/logical_index_scan.h"
#include "planner/planner.h"

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

std::unique_ptr<LogicalPlan> Planner::planCopyFrom(const BoundStatement& statement) {
    auto& copyFrom = reinterpret_cast<const BoundCopyFrom&>(statement);
    auto copyFromInfo = copyFrom.getInfo();
    auto fileType = copyFromInfo->fileScanInfo->readerConfig->fileType;
    auto plan = std::make_unique<LogicalPlan>();
    QueryPlanner::appendScanFile(copyFromInfo->fileScanInfo.get(), *plan);
    auto tableType = copyFromInfo->tableSchema->tableType;
    if (tableType == TableType::REL) {
        if (fileType == FileType::TURTLE) {
            auto extraInfo =
                reinterpret_cast<ExtraBoundCopyRdfRelInfo*>(copyFromInfo->extraInfo.get());
            std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> infos;
            auto pkType = LogicalType(LogicalTypeID::STRING);
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(extraInfo->nodeTableID,
                extraInfo->subjectOffset, extraInfo->subjectKey, pkType.copy()));
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(extraInfo->nodeTableID,
                extraInfo->predicateOffset, extraInfo->predicateKey, pkType.copy()));
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(extraInfo->nodeTableID,
                extraInfo->objectOffset, extraInfo->objectKey, pkType.copy()));
            appendIndexScan(std::move(infos), *plan);
        } else {
            auto extraInfo =
                reinterpret_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
            std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> infos;
            auto srcNodeTableSchema = reinterpret_cast<NodeTableSchema*>(extraInfo->srcTableSchema);
            auto srcTableID = srcNodeTableSchema->getTableID();
            auto srcPkType = srcNodeTableSchema->getPrimaryKey()->getDataType();
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(
                srcTableID, extraInfo->srcOffset, extraInfo->srcKey, srcPkType->copy()));
            auto dstNodeTableSchema = reinterpret_cast<NodeTableSchema*>(extraInfo->dstTableSchema);
            auto dstTableID = dstNodeTableSchema->getTableID();
            auto dstPkType = dstNodeTableSchema->getPrimaryKey()->getDataType();
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(
                dstTableID, extraInfo->dstOffset, extraInfo->dstKey, dstPkType->copy()));
            appendIndexScan(std::move(infos), *plan);
        }
    }
    auto copy = make_shared<LogicalCopyFrom>(copyFromInfo->copy(),
        copyFrom.getStatementResult()->getSingleExpressionToCollect(), plan->getLastOperator());
    copy->computeFactorizedSchema();
    plan->setLastOperator(std::move(copy));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyTo(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    auto& copyClause = reinterpret_cast<const BoundCopyTo&>(statement);
    auto regularQuery = copyClause.getRegularQuery();
    assert(regularQuery->getStatementType() == StatementType::QUERY);
    auto plan = QueryPlanner(catalog, nodesStatistics, relsStatistics).getBestPlan(*regularQuery);
    auto logicalCopyTo =
        make_shared<LogicalCopyTo>(plan->getLastOperator(), copyClause.getConfig()->copy());
    plan->setLastOperator(std::move(logicalCopyTo));
    return plan;
}

} // namespace planner
} // namespace kuzu
