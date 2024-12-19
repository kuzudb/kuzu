#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "planner/operator/logical_partitioner.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "processor/operator/index_lookup.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/node_batch_insert.h"
#include "processor/operator/persistent/rel_batch_insert.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyFrom(LogicalOperator* logicalOperator) {
    const auto& copyFrom = logicalOperator->cast<LogicalCopyFrom>();
    clientContext->getWarningContextUnsafe().setIgnoreErrorsForCurrentQuery(
        copyFrom.getInfo()->source->getIgnoreErrorsOption());
    switch (copyFrom.getInfo()->tableEntry->getTableType()) {
    case TableType::NODE: {
        auto op = mapCopyNodeFrom(logicalOperator);
        const auto copy = op->ptrCast<NodeBatchInsert>();
        const auto table = copy->getSharedState()->fTable;
        physical_op_vector_t children;
        children.push_back(std::move(op));
        return createFTableScanAligned(copyFrom.getOutExprs(), copyFrom.getSchema(), table,
            DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(children));
    }
    case TableType::REL: {
        auto ops = mapCopyRelFrom(logicalOperator);
        const auto relBatchInsert = ops[0]->ptrCast<RelBatchInsert>();
        const auto fTable = relBatchInsert->getSharedState()->fTable;
        physical_op_vector_t children;
        children.push_back(std::move(ops[0]));
        auto scan = createFTableScanAligned(copyFrom.getOutExprs(), copyFrom.getSchema(), fTable,
            DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(children));
        for (auto i = 1u; i < ops.size(); ++i) {
            scan->addChild(std::move(ops[i]));
        }
        return scan;
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNodeFrom(LogicalOperator* logicalOperator) {
    const auto storageManager = clientContext->getStorageManager();
    auto& copyFrom = logicalOperator->constCast<LogicalCopyFrom>();
    const auto copyFromInfo = copyFrom.getInfo();
    const auto outFSchema = copyFrom.getSchema();
    auto nodeTableEntry = copyFromInfo->tableEntry->ptrCast<NodeTableCatalogEntry>();
    // Map reader.
    auto prevOperator = mapOperator(copyFrom.getChild(0).get());
    auto nodeTable = storageManager->getTable(nodeTableEntry->getTableID());
    auto fTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    const auto& pkDefinition = nodeTableEntry->getPrimaryKeyDefinition();
    auto pkColumnID = nodeTableEntry->getColumnID(pkDefinition.getName());
    auto sharedState = std::make_shared<NodeBatchInsertSharedState>(nodeTable, pkColumnID,
        pkDefinition.getType().copy(), fTable, &storageManager->getWAL(),
        clientContext->getMemoryManager());

    if (prevOperator->getOperatorType() == PhysicalOperatorType::TABLE_FUNCTION_CALL) {
        const auto call = prevOperator->ptrCast<TableFunctionCall>();
        sharedState->readerSharedState = call->getSharedState();
    }
    // Map copy node.
    std::vector<LogicalType> columnTypes;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnEvaluators;
    auto exprMapper = ExpressionMapper(outFSchema);
    for (auto& expr : copyFromInfo->columnExprs) {
        columnTypes.push_back(expr->getDataType().copy());
        columnEvaluators.push_back(exprMapper.getEvaluator(expr));
    }

    const auto numWarningDataColumns = copyFromInfo->source->getNumWarningDataColumns();
    KU_ASSERT(columnTypes.size() >= numWarningDataColumns);
    auto info = std::make_unique<NodeBatchInsertInfo>(nodeTableEntry,
        storageManager->compressionEnabled(), std::move(columnTypes), std::move(columnEvaluators),
        copyFromInfo->columnEvaluateTypes, numWarningDataColumns);

    auto printInfo =
        std::make_unique<NodeBatchInsertPrintInfo>(copyFrom.getInfo()->tableEntry->getName());
    return std::make_unique<NodeBatchInsert>(std::move(info), sharedState,
        std::make_unique<ResultSetDescriptor>(copyFrom.getSchema()), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapPartitioner(LogicalOperator* logicalOperator) {
    auto& logicalPartitioner = logicalOperator->constCast<LogicalPartitioner>();
    auto prevOperator = mapOperator(logicalPartitioner.getChild(0).get());
    auto outFSchema = logicalPartitioner.getSchema();
    auto& copyFromInfo = logicalPartitioner.copyFromInfo;
    auto& extraInfo = copyFromInfo.extraInfo->constCast<ExtraBoundCopyRelInfo>();
    PartitionerInfo partitionerInfo;
    partitionerInfo.relOffsetDataPos =
        getDataPos(*logicalPartitioner.getInfo().offset, *outFSchema);
    partitionerInfo.infos.reserve(logicalPartitioner.getInfo().getNumInfos());
    for (auto i = 0u; i < logicalPartitioner.getInfo().getNumInfos(); i++) {
        partitionerInfo.infos.emplace_back(logicalPartitioner.getInfo().getInfo(i).keyIdx,
            PartitionerFunctions::partitionRelData);
    }
    std::vector<LogicalType> columnTypes;
    evaluator::evaluator_vector_t columnEvaluators;
    auto exprMapper = ExpressionMapper(outFSchema);
    for (auto& expr : copyFromInfo.columnExprs) {
        columnTypes.push_back(expr->getDataType().copy());
        columnEvaluators.push_back(exprMapper.getEvaluator(expr));
    }
    for (auto idx : extraInfo.internalIDColumnIndices) {
        columnTypes[idx] = LogicalType::INTERNAL_ID();
    }
    auto dataInfo = PartitionerDataInfo(LogicalType::copy(columnTypes), std::move(columnEvaluators),
        copyFromInfo.columnEvaluateTypes);
    auto sharedState = std::make_shared<PartitionerSharedState>(*clientContext->getMemoryManager());
    binder::expression_vector expressions;
    for (auto& info : partitionerInfo.infos) {
        expressions.push_back(copyFromInfo.columnExprs[info.keyIdx]);
    }
    auto printInfo = std::make_unique<PartitionerPrintInfo>(expressions);
    return std::make_unique<Partitioner>(std::make_unique<ResultSetDescriptor>(outFSchema),
        std::move(partitionerInfo), std::move(dataInfo), std::move(sharedState),
        std::move(prevOperator), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createCopyRel(
    std::shared_ptr<PartitionerSharedState> partitionerSharedState,
    std::shared_ptr<BatchInsertSharedState> sharedState, const LogicalCopyFrom& copyFrom,
    RelDataDirection direction, std::vector<LogicalType> columnTypes) {
    const auto copyFromInfo = copyFrom.getInfo();
    auto outFSchema = copyFrom.getSchema();
    auto partitioningIdx = direction == RelDataDirection::FWD ? 0 : 1;
    auto offsetVectorIdx = direction == RelDataDirection::FWD ? 0 : 1;

    const auto numWarningDataColumns = copyFromInfo->source->getNumWarningDataColumns();
    KU_ASSERT(numWarningDataColumns <= copyFromInfo->columnExprs.size());
    for (column_id_t i = numWarningDataColumns; i >= 1; --i) {
        columnTypes.push_back(
            copyFromInfo->columnExprs[copyFromInfo->columnExprs.size() - i]->getDataType().copy());
    }

    auto relBatchInsertInfo = std::make_unique<RelBatchInsertInfo>(copyFromInfo->tableEntry,
        clientContext->getStorageManager()->compressionEnabled(), direction, partitioningIdx,
        offsetVectorIdx, std::move(columnTypes), numWarningDataColumns);
    auto printInfo =
        std::make_unique<RelBatchInsertPrintInfo>(copyFrom.getInfo()->tableEntry->getName());
    return std::make_unique<RelBatchInsert>(std::move(relBatchInsertInfo),
        std::move(partitionerSharedState), std::move(sharedState),
        std::make_unique<ResultSetDescriptor>(outFSchema), getOperatorID(), std::move(printInfo));
}

physical_op_vector_t PlanMapper::mapCopyRelFrom(LogicalOperator* logicalOperator) {
    auto& copyFrom = logicalOperator->constCast<LogicalCopyFrom>();
    const auto copyFromInfo = copyFrom.getInfo();
    auto& relTableEntry = copyFromInfo->tableEntry->constCast<RelTableCatalogEntry>();
    auto partitioner = mapOperator(copyFrom.getChild(0).get());
    KU_ASSERT(partitioner->getOperatorType() == PhysicalOperatorType::PARTITIONER);
    const auto partitionerSharedState = partitioner->ptrCast<Partitioner>()->getSharedState();
    const auto storageManager = clientContext->getStorageManager();
    partitionerSharedState->srcNodeTable =
        storageManager->getTable(relTableEntry.getSrcTableID())->ptrCast<NodeTable>();
    partitionerSharedState->dstNodeTable =
        storageManager->getTable(relTableEntry.getDstTableID())->ptrCast<NodeTable>();
    partitionerSharedState->relTable =
        storageManager->getTable(relTableEntry.getTableID())->ptrCast<RelTable>();
    auto relTable = storageManager->getTable(relTableEntry.getTableID());
    // TODO(Xiyang): Move binding of column types to binder.
    std::vector<LogicalType> columnTypes;
    columnTypes.push_back(LogicalType::INTERNAL_ID()); // NBR_ID COLUMN.
    for (auto& property : relTableEntry.getProperties()) {
        columnTypes.push_back(property.getType().copy());
    }
    auto fTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    const auto batchInsertSharedState = std::make_shared<BatchInsertSharedState>(relTable, fTable,
        &storageManager->getWAL(), clientContext->getMemoryManager());
    auto copyRelFWD = createCopyRel(partitionerSharedState, batchInsertSharedState, copyFrom,
        RelDataDirection::FWD, LogicalType::copy(columnTypes));
    auto copyRelBWD = createCopyRel(partitionerSharedState, batchInsertSharedState, copyFrom,
        RelDataDirection::BWD, std::move(columnTypes));
    physical_op_vector_t result;
    result.push_back(std::move(copyRelBWD));
    result.push_back(std::move(copyRelFWD));
    result.push_back(std::move(partitioner));
    return result;
}

} // namespace processor
} // namespace kuzu
