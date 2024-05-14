#include "catalog/catalog_entry/table_catalog_entry.h"
#include "planner/operator/logical_partitioner.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "processor/operator/aggregate/hash_aggregate_scan.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/index_lookup.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/copy_rdf.h"
#include "processor/operator/persistent/node_batch_insert.h"
#include "processor/operator/persistent/rel_batch_insert.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    switch (copyFrom->getInfo()->tableEntry->getTableType()) {
    case TableType::NODE: {
        auto op = mapCopyNodeFrom(logicalOperator);
        auto copy = ku_dynamic_cast<PhysicalOperator*, NodeBatchInsert*>(op.get());
        auto table = copy->getSharedState()->fTable;
        return createFTableScanAligned(copyFrom->getOutExprs(), copyFrom->getSchema(), table,
            DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(op));
    }
    case TableType::REL: {
        auto ops = mapCopyRelFrom(logicalOperator);
        auto relBatchInsert = ku_dynamic_cast<PhysicalOperator*, RelBatchInsert*>(ops[0].get());
        auto fTable = relBatchInsert->getSharedState()->fTable;
        auto scan = createFTableScanAligned(copyFrom->getOutExprs(), copyFrom->getSchema(), fTable,
            DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(ops[0]));
        for (auto i = 1u; i < ops.size(); ++i) {
            scan->addChild(std::move(ops[i]));
        }
        return scan;
    }
    case TableType::RDF:
        return mapCopyRdfFrom(logicalOperator);
    default:
        KU_UNREACHABLE;
    }
}

static void getNodeColumnsInCopyOrder(TableCatalogEntry* tableEntry,
    std::vector<std::string>& columnNames, std::vector<LogicalType>& columnTypes) {
    for (auto& property : tableEntry->getPropertiesRef()) {
        columnNames.push_back(property.getName());
        columnTypes.push_back(*property.getDataType()->copy());
    }
}

static void getRelColumnNamesInCopyOrder(TableCatalogEntry* tableEntry,
    std::vector<std::string>& columnNames, std::vector<LogicalType>& columnTypes) {
    columnNames.emplace_back(InternalKeyword::SRC_OFFSET);
    columnNames.emplace_back(InternalKeyword::DST_OFFSET);
    columnNames.emplace_back(InternalKeyword::ROW_OFFSET);
    columnTypes.emplace_back(LogicalType(LogicalTypeID::INTERNAL_ID));
    columnTypes.emplace_back(LogicalType(LogicalTypeID::INTERNAL_ID));
    columnTypes.emplace_back(LogicalType(LogicalTypeID::INTERNAL_ID));
    auto& properties = tableEntry->getPropertiesRef();
    for (auto i = 1u; i < properties.size(); ++i) { // skip internal ID
        columnNames.push_back(properties[i].getName());
        columnTypes.push_back(*properties[i].getDataType()->copy());
    }
}

static std::shared_ptr<Expression> matchColumnExpression(const expression_vector& columnExpressions,
    const std::string& columnName) {
    for (auto& expression : columnExpressions) {
        if (columnName == expression->toString()) {
            return expression;
        }
    }
    return nullptr;
}

static std::vector<DataPos> getColumnDataPositions(const std::vector<std::string>& columnNames,
    const expression_vector& inputColumns, const Schema& fSchema) {
    std::vector<DataPos> columnPositions;
    for (auto& columnName : columnNames) {
        auto expr = matchColumnExpression(inputColumns, columnName);
        if (expr != nullptr) {
            columnPositions.emplace_back(fSchema.getExpressionPos(*expr));
        } else {
            columnPositions.push_back(DataPos());
        }
    }
    return columnPositions;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNodeFrom(LogicalOperator* logicalOperator) {
    auto storageManager = clientContext->getStorageManager();
    auto copyFrom = ku_dynamic_cast<LogicalOperator*, LogicalCopyFrom*>(logicalOperator);
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto nodeTableEntry =
        ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(copyFromInfo->tableEntry);
    // Map reader.
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    auto nodeTable = storageManager->getTable(nodeTableEntry->getTableID());
    auto fTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    auto sharedState =
        std::make_shared<NodeBatchInsertSharedState>(nodeTable, fTable, &storageManager->getWAL());
    if (prevOperator->getOperatorType() == PhysicalOperatorType::IN_QUERY_CALL) {
        auto inQueryCall = ku_dynamic_cast<PhysicalOperator*, InQueryCall*>(prevOperator.get());
        sharedState->readerSharedState = inQueryCall->getSharedState();
    } else {
        KU_ASSERT(prevOperator->getOperatorType() == PhysicalOperatorType::AGGREGATE_SCAN);
        auto hashScan = ku_dynamic_cast<PhysicalOperator*, HashAggregateScan*>(prevOperator.get());
        sharedState->distinctSharedState = hashScan->getSharedState().get();
    }
    // Map copy node.
    auto pk = nodeTableEntry->getPrimaryKey();
    sharedState->pkColumnIdx = nodeTableEntry->getColumnID(pk->getPropertyID());
    sharedState->pkType = *pk->getDataType();
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    getNodeColumnsInCopyOrder(nodeTableEntry, columnNames, columnTypes);
    std::vector<std::string> columnNamesExcludingSerial;
    for (auto i = 0u; i < columnNames.size(); ++i) {
        if (columnTypes[i].getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            continue;
        }
        columnNamesExcludingSerial.push_back(columnNames[i]);
    }
    auto inputColumns = copyFromInfo->source->getColumns();
    inputColumns.push_back(copyFromInfo->offset);
    auto columnPositions =
        getColumnDataPositions(columnNamesExcludingSerial, inputColumns, *outFSchema);
    auto containsSerial = nodeTableEntry->containPropertyType(*LogicalType::SERIAL());
    auto info =
        std::make_unique<NodeBatchInsertInfo>(nodeTableEntry, storageManager->compressionEnabled(),
            std::move(columnPositions), containsSerial, std::move(columnTypes));
    return std::make_unique<NodeBatchInsert>(std::move(info), sharedState,
        std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
        getOperatorID(), copyFrom->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapPartitioner(LogicalOperator* logicalOperator) {
    auto logicalPartitioner =
        ku_dynamic_cast<LogicalOperator*, LogicalPartitioner*>(logicalOperator);
    auto prevOperator = mapOperator(logicalPartitioner->getChild(0).get());
    auto outFSchema = logicalPartitioner->getSchema();
    std::vector<std::unique_ptr<PartitioningInfo>> infos;
    infos.reserve(logicalPartitioner->getNumInfos());
    for (auto i = 0u; i < logicalPartitioner->getNumInfos(); i++) {
        auto info = logicalPartitioner->getInfo(i);
        auto keyPos = getDataPos(*info->key, *outFSchema);
        std::vector<std::string> columnNames;
        std::vector<LogicalType> columnTypes;
        getRelColumnNamesInCopyOrder(info->tableEntry, columnNames, columnTypes);
        auto columnPositions = getColumnDataPositions(columnNames, info->payloads, *outFSchema);
        infos.push_back(std::make_unique<PartitioningInfo>(keyPos, columnPositions,
            std::move(columnTypes), PartitionerFunctions::partitionRelData));
    }
    std::vector<common::logical_type_vec_t> columnTypes;
    for (auto& info : infos) {
        columnTypes.push_back(info->columnTypes);
    }
    auto sharedState = std::make_shared<PartitionerSharedState>(std::move(columnTypes));
    return std::make_unique<Partitioner>(std::make_unique<ResultSetDescriptor>(outFSchema),
        std::move(infos), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        logicalPartitioner->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::createCopyRel(
    std::shared_ptr<PartitionerSharedState> partitionerSharedState,
    std::shared_ptr<BatchInsertSharedState> sharedState, LogicalCopyFrom* copyFrom,
    RelDataDirection direction, std::vector<common::LogicalType> columnTypes) {
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto partitioningIdx = direction == RelDataDirection::FWD ? 0 : 1;
    auto offsetVectorIdx = direction == RelDataDirection::FWD ? 0 : 1;
    auto relBatchInsertInfo = std::make_unique<RelBatchInsertInfo>(copyFromInfo->tableEntry,
        clientContext->getStorageManager()->compressionEnabled(), direction, partitioningIdx,
        offsetVectorIdx, std::move(columnTypes));
    return std::make_unique<RelBatchInsert>(std::move(relBatchInsertInfo),
        std::move(partitionerSharedState), std::move(sharedState),
        std::make_unique<ResultSetDescriptor>(outFSchema), getOperatorID(),
        copyFrom->getExpressionsForPrinting());
}

physical_op_vector_t PlanMapper::mapCopyRelFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto relTableEntry =
        ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(copyFromInfo->tableEntry);
    auto partitioner = mapOperator(copyFrom->getChild(0).get());
    KU_ASSERT(partitioner->getOperatorType() == PhysicalOperatorType::PARTITIONER);
    auto partitionerSharedState = dynamic_cast<Partitioner*>(partitioner.get())->getSharedState();
    auto storageManager = clientContext->getStorageManager();
    partitionerSharedState->srcNodeTable = ku_dynamic_cast<Table*, NodeTable*>(
        storageManager->getTable(relTableEntry->getSrcTableID()));
    partitionerSharedState->dstNodeTable = ku_dynamic_cast<Table*, NodeTable*>(
        storageManager->getTable(relTableEntry->getDstTableID()));
    // TODO(Guodong/Xiyang): This is a temp hack to set rel offset.
    KU_ASSERT(partitioner->getChild(0)->getChild(0)->getOperatorType() ==
              PhysicalOperatorType::IN_QUERY_CALL);
    auto scanFile =
        ku_dynamic_cast<PhysicalOperator*, InQueryCall*>(partitioner->getChild(0)->getChild(0));
    auto relTable = storageManager->getTable(relTableEntry->getTableID());
    scanFile->getSharedState()->nextRowIdx =
        relTable->getNumTuples(&transaction::DUMMY_WRITE_TRANSACTION);
    // TODO(Xiyang): Move binding of column types to binder.
    std::vector<LogicalType> columnTypes;
    columnTypes.push_back(*LogicalType::INTERNAL_ID()); // NBR_ID COLUMN.
    for (auto& property : relTableEntry->getPropertiesRef()) {
        columnTypes.push_back(*property.getDataType()->copy());
    }
    auto fTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    auto batchInsertSharedState =
        std::make_shared<BatchInsertSharedState>(relTable, fTable, &storageManager->getWAL());
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyRdfFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = ku_dynamic_cast<LogicalOperator*, LogicalCopyFrom*>(logicalOperator);
    auto logicalRRLChild = logicalOperator->getChild(0).get();
    auto logicalRRRChild = logicalOperator->getChild(1).get();
    auto logicalLChild = logicalOperator->getChild(2).get();
    auto logicalRChild = logicalOperator->getChild(3).get();
    std::unique_ptr<PhysicalOperator> scanChild;
    if (logicalOperator->getNumChildren() > 4) {
        auto logicalScanChild = logicalOperator->getChild(4).get();
        scanChild = mapOperator(logicalScanChild);
        scanChild = createResultCollector(AccumulateType::REGULAR, expression_vector{},
            logicalScanChild->getSchema(), std::move(scanChild));
    }

    auto rChild = mapCopyNodeFrom(logicalRChild);
    KU_ASSERT(rChild->getOperatorType() == PhysicalOperatorType::BATCH_INSERT);
    auto rCopy = ku_dynamic_cast<PhysicalOperator*, NodeBatchInsert*>(rChild.get());
    auto lChild = mapCopyNodeFrom(logicalLChild);
    auto lCopy = ku_dynamic_cast<PhysicalOperator*, NodeBatchInsert*>(lChild.get());
    auto rrrChildren = mapCopyRelFrom(logicalRRRChild);
    KU_ASSERT(rrrChildren[2]->getOperatorType() == PhysicalOperatorType::PARTITIONER);
    auto rrrPartitioner = ku_dynamic_cast<PhysicalOperator*, Partitioner*>(rrrChildren[2].get());
    rrrPartitioner->getSharedState()->nodeBatchInsertSharedStates.push_back(
        rCopy->getSharedState());
    rrrPartitioner->getSharedState()->nodeBatchInsertSharedStates.push_back(
        rCopy->getSharedState());
    KU_ASSERT(rrrChildren[2]->getChild(0)->getOperatorType() == PhysicalOperatorType::INDEX_LOOKUP);
    auto rrrLookup = ku_dynamic_cast<PhysicalOperator*, IndexLookup*>(rrrChildren[2]->getChild(0));
    rrrLookup->setBatchInsertSharedState(rCopy->getSharedState());
    auto rrlChildren = mapCopyRelFrom(logicalRRLChild);
    auto rrLPartitioner = ku_dynamic_cast<PhysicalOperator*, Partitioner*>(rrlChildren[2].get());
    rrLPartitioner->getSharedState()->nodeBatchInsertSharedStates.push_back(
        rCopy->getSharedState());
    rrLPartitioner->getSharedState()->nodeBatchInsertSharedStates.push_back(
        lCopy->getSharedState());
    KU_ASSERT(rrlChildren[2]->getChild(0)->getOperatorType() == PhysicalOperatorType::INDEX_LOOKUP);
    auto rrlLookup = ku_dynamic_cast<PhysicalOperator*, IndexLookup*>(rrlChildren[2]->getChild(0));
    rrlLookup->setBatchInsertSharedState(rCopy->getSharedState());
    auto sharedState = std::make_shared<CopyRdfSharedState>();
    auto fTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    sharedState->fTable = fTable;
    auto copyRdf = std::make_unique<CopyRdf>(std::move(sharedState),
        std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), getOperatorID(), "");
    for (auto& child : rrlChildren) {
        copyRdf->addChild(std::move(child));
    }
    for (auto& child : rrrChildren) {
        copyRdf->addChild(std::move(child));
    }
    copyRdf->addChild(std::move(lChild));
    copyRdf->addChild(std::move(rChild));
    if (scanChild != nullptr) {
        copyRdf->addChild(std::move(scanChild));
    }
    return createFTableScanAligned(copyFrom->getOutExprs(), copyFrom->getSchema(), fTable,
        DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(copyRdf));
}

} // namespace processor
} // namespace kuzu
