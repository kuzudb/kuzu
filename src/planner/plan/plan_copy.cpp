#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "planner/operator/logical_partitioner.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/scan/logical_index_scan.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace planner {

static void appendIndexScan(std::vector<IndexLookupInfo> infos, LogicalPlan& plan) {
    auto indexScan =
        std::make_shared<LogicalIndexScanNode>(std::move(infos), plan.getLastOperator());
    indexScan->computeFactorizedSchema();
    plan.setLastOperator(std::move(indexScan));
}

static void appendPartitioner(const BoundCopyFromInfo& copyFromInfo, LogicalPlan& plan) {
    auto extraInfo = ku_dynamic_cast<ExtraBoundCopyFromInfo*, ExtraBoundCopyRelInfo*>(
        copyFromInfo.extraInfo.get());
    expression_vector payloads;
    for (auto& column : extraInfo->propertyColumns) {
        payloads.push_back(column);
    }
    payloads.push_back(copyFromInfo.offset);
    for (auto& lookupInfo : extraInfo->infos) {
        payloads.push_back(lookupInfo.offset);
    }
    std::vector<std::unique_ptr<LogicalPartitionerInfo>> infos;
    // Partitioner for FWD direction rel data.
    auto relTableEntry =
        ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(copyFromInfo.tableEntry);
    infos.push_back(std::make_unique<LogicalPartitionerInfo>(extraInfo->fromOffset, payloads,
        relTableEntry->isSingleMultiplicity(RelDataDirection::FWD) ? ColumnDataFormat::REGULAR :
                                                                     ColumnDataFormat::CSR,
        relTableEntry));
    // Partitioner for BWD direction rel data.
    infos.push_back(std::make_unique<LogicalPartitionerInfo>(extraInfo->toOffset, payloads,
        relTableEntry->isSingleMultiplicity(RelDataDirection::BWD) ? ColumnDataFormat::REGULAR :
                                                                     ColumnDataFormat::CSR,
        relTableEntry));
    auto partitioner =
        std::make_shared<LogicalPartitioner>(std::move(infos), plan.getLastOperator());
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
    auto& copyFrom = ku_dynamic_cast<const BoundStatement&, const BoundCopyFrom&>(statement);
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
    case TableType::RDF: {
        return planCopyRdfFrom(copyFromInfo, outExprs);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<LogicalPlan> Planner::planCopyNodeFrom(const BoundCopyFromInfo* info,
    binder::expression_vector results) {
    auto plan = std::make_unique<LogicalPlan>();
    switch (info->source->type) {
    case ScanSourceType::FILE: {
        auto fileSource =
            ku_dynamic_cast<BoundBaseScanSource*, BoundFileScanSource*>(info->source.get());
        appendScanFile(&fileSource->fileScanInfo, *plan);
    } break;
    case ScanSourceType::QUERY: {
        auto querySource =
            ku_dynamic_cast<BoundBaseScanSource*, BoundQueryScanSource*>(info->source.get());
        plan = getBestPlan(planQuery(*querySource->statement));
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
    binder::expression_vector results) {
    auto plan = std::make_unique<LogicalPlan>();
    KU_ASSERT(info->source->type == ScanSourceType::FILE);
    auto fileSource =
        ku_dynamic_cast<BoundBaseScanSource*, BoundFileScanSource*>(info->source.get());
    appendScanFile(&fileSource->fileScanInfo, *plan);
    appendDistinct(fileSource->fileScanInfo.columns, *plan);
    appendCopyFrom(*info, results, *plan);
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyRelFrom(const BoundCopyFromInfo* info,
    binder::expression_vector results) {
    auto plan = std::make_unique<LogicalPlan>();
    switch (info->source->type) {
    case ScanSourceType::FILE: {
        auto fileSource =
            ku_dynamic_cast<BoundBaseScanSource*, BoundFileScanSource*>(info->source.get());
        appendScanFile(&fileSource->fileScanInfo, info->offset, *plan);
    } break;
    case ScanSourceType::QUERY: {
        auto querySource =
            ku_dynamic_cast<BoundBaseScanSource*, BoundQueryScanSource*>(info->source.get());
        plan = getBestPlan(planQuery(*querySource->statement));
        appendAccumulate(AccumulateType::REGULAR, plan->getSchema()->getExpressionsInScope(),
            info->offset, nullptr /* mark */, *plan);
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto extraInfo =
        ku_dynamic_cast<ExtraBoundCopyFromInfo*, ExtraBoundCopyRelInfo*>(info->extraInfo.get());
    appendIndexScan(copyVector(extraInfo->infos), *plan);
    appendPartitioner(*info, *plan);
    appendCopyFrom(*info, results, *plan);
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyRdfFrom(const BoundCopyFromInfo* info,
    binder::expression_vector results) {
    auto extraRdfInfo =
        ku_dynamic_cast<ExtraBoundCopyFromInfo*, ExtraBoundCopyRdfInfo*>(info->extraInfo.get());
    auto rPlan = planCopyResourceFrom(&extraRdfInfo->rInfo, results);
    auto lPlan = planCopyNodeFrom(&extraRdfInfo->lInfo, results);
    auto rrrPlan = planCopyRelFrom(&extraRdfInfo->rrrInfo, results);
    auto rrlPlan = planCopyRelFrom(&extraRdfInfo->rrlInfo, results);
    auto children = logical_op_vector_t{rrlPlan->getLastOperator(), rrrPlan->getLastOperator(),
        lPlan->getLastOperator(), rPlan->getLastOperator()};
    if (info->source->type == ScanSourceType::FILE) {
        auto readerPlan = LogicalPlan();
        auto fileSource =
            ku_dynamic_cast<BoundBaseScanSource*, BoundFileScanSource*>(info->source.get());
        appendScanFile(&fileSource->fileScanInfo, readerPlan);
        children.push_back(readerPlan.getLastOperator());
    }
    auto resultPlan = std::make_unique<LogicalPlan>();
    auto op = make_shared<LogicalCopyFrom>(info->copy(), results, children);
    op->computeFactorizedSchema();
    resultPlan->setLastOperator(std::move(op));
    return resultPlan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyTo(const BoundStatement& statement) {
    auto& boundCopy = ku_dynamic_cast<const BoundStatement&, const BoundCopyTo&>(statement);
    auto regularQuery = boundCopy.getRegularQuery();
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    for (auto& column : regularQuery->getStatementResult()->getColumns()) {
        columnNames.push_back(column->toString());
        columnTypes.push_back(column->dataType);
    }
    KU_ASSERT(regularQuery->getStatementType() == StatementType::QUERY);
    auto plan = getBestPlan(*regularQuery);
    auto copyTo = make_shared<LogicalCopyTo>(boundCopy.getFilePath(), boundCopy.getFileType(),
        columnNames, columnTypes, boundCopy.getCopyOption()->copy(), plan->getLastOperator());
    plan->setLastOperator(std::move(copyTo));
    return plan;
}

} // namespace planner
} // namespace kuzu
