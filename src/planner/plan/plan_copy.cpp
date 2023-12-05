#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/rel_table_schema.h"
#include "common/copier_config/rdf_reader_config.h"
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
using namespace kuzu::function;

namespace kuzu {
namespace planner {

static void appendIndexScan(
    std::vector<std::unique_ptr<IndexLookupInfo>> infos, LogicalPlan& plan) {
    auto indexScan =
        std::make_shared<LogicalIndexScanNode>(std::move(infos), plan.getLastOperator());
    indexScan->computeFactorizedSchema();
    plan.setLastOperator(std::move(indexScan));
}

static void appendPartitioner(BoundCopyFromInfo* copyFromInfo, LogicalPlan& plan) {
    auto extraInfo = reinterpret_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
    expression_vector payloads;
    for (auto& column : extraInfo->propertyColumns) {
        payloads.push_back(column);
    }
    payloads.push_back(copyFromInfo->fileScanInfo->offset);
    for (auto& lookupInfo : extraInfo->infos) {
        payloads.push_back(lookupInfo->offset);
    }
    std::vector<std::unique_ptr<LogicalPartitionerInfo>> infos;
    // Partitioner for FWD direction rel data.
    auto relSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    infos.push_back(std::make_unique<LogicalPartitionerInfo>(extraInfo->fromOffset, payloads,
        relSchema->isSingleMultiplicity(RelDataDirection::FWD) ? ColumnDataFormat::REGULAR :
                                                                 ColumnDataFormat::CSR,
        relSchema));
    // Partitioner for BWD direction rel data.
    infos.push_back(std::make_unique<LogicalPartitionerInfo>(extraInfo->toOffset, payloads,
        relSchema->isSingleMultiplicity(RelDataDirection::BWD) ? ColumnDataFormat::REGULAR :
                                                                 ColumnDataFormat::CSR,
        relSchema));
    auto partitioner =
        std::make_shared<LogicalPartitioner>(std::move(infos), plan.getLastOperator());
    partitioner->computeFactorizedSchema();
    plan.setLastOperator(std::move(partitioner));
}

static void appendCopyFrom(const BoundCopyFrom& copyFrom, LogicalPlan& plan) {
    auto copyFromInfo = copyFrom.getInfo();
    auto op = make_shared<LogicalCopyFrom>(copyFromInfo->copy(),
        copyFrom.getStatementResult()->getSingleColumnExpr(), plan.getLastOperator());
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
}

static void planCopyNode(const BoundCopyFrom& copyFrom, LogicalPlan& plan,
    catalog::Catalog* catalog, storage::StorageManager* storageManager) {
    auto copyFromInfo = copyFrom.getInfo();
    auto scanInfo = copyFromInfo->fileScanInfo.get();
    auto readerConfig = reinterpret_cast<ScanBindData*>(scanInfo->bindData.get())->config;
    auto fileType = readerConfig.fileType;
    if (FileTypeUtils::isRdf(fileType)) {
        auto extraInfo = reinterpret_cast<RdfReaderConfig*>(readerConfig.extraConfig.get());
        if (extraInfo->mode == common::RdfReaderMode::RESOURCE) {
            auto planner = QueryPlanner(*catalog, storageManager);
            KU_ASSERT(scanInfo->columns.size() == 1);
            planner.appendDistinct(scanInfo->columns, plan);
        }
    }
    appendCopyFrom(copyFrom, plan);
}

static void planCopyRel(const BoundCopyFrom& copyFrom, LogicalPlan& plan) {
    auto copyFromInfo = copyFrom.getInfo();
    auto scanInfo = copyFromInfo->fileScanInfo.get();
    auto readerConfig = reinterpret_cast<ScanBindData*>(scanInfo->bindData.get())->config;
    auto extraInfo = dynamic_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
    appendIndexScan(IndexLookupInfo::copy(extraInfo->infos), plan);
    appendPartitioner(copyFromInfo, plan);
    appendCopyFrom(copyFrom, plan);
}

std::unique_ptr<LogicalPlan> Planner::planCopyFrom(const BoundStatement& statement) {
    auto& copyFrom = dynamic_cast<const BoundCopyFrom&>(statement);
    auto copyFromInfo = copyFrom.getInfo();
    auto plan = std::make_unique<LogicalPlan>();
    QueryPlanner::appendScanFile(copyFromInfo->fileScanInfo.get(), *plan);
    auto tableType = copyFromInfo->tableSchema->tableType;
    if (tableType == TableType::REL) {
        planCopyRel(copyFrom, *plan);
    } else {
        KU_ASSERT(tableType == TableType::NODE);
        planCopyNode(copyFrom, *plan, catalog, storageManager);
    }
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyTo(const BoundStatement& statement) {
    auto& boundCopy = reinterpret_cast<const BoundCopyTo&>(statement);
    auto regularQuery = boundCopy.getRegularQuery();
    KU_ASSERT(regularQuery->getStatementType() == StatementType::QUERY);
    auto plan = QueryPlanner(*catalog, storageManager).getBestPlan(*regularQuery);
    auto copyTo = make_shared<LogicalCopyTo>(boundCopy.getFilePath(), boundCopy.getFileType(),
        boundCopy.getColumnNames(), LogicalType::copy(boundCopy.getColumnTypesRef()),
        std::make_unique<common::CSVOption>(*boundCopy.getCopyOption()), plan->getLastOperator());
    plan->setLastOperator(std::move(copyTo));
    return plan;
}

} // namespace planner
} // namespace kuzu
