#include "binder/expression/variable_expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "planner/operator/logical_partitioner.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "processor/operator/aggregate/hash_aggregate_scan.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/index_lookup.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/copy_node.h"
#include "processor/operator/persistent/copy_rdf.h"
#include "processor/operator/persistent/copy_rel.h"
#include "processor/plan_mapper.h"

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
        auto copy = ku_dynamic_cast<PhysicalOperator*, CopyNode*>(op.get());
        auto table = copy->getSharedState()->fTable;
        return createFactorizedTableScanAligned(copyFrom->getOutExprs(), copyFrom->getSchema(),
            table, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(op));
    }
    case TableType::REL: {
        auto ops = mapCopyRelFrom(logicalOperator);
        auto copy = ku_dynamic_cast<PhysicalOperator*, CopyRel*>(ops[0].get());
        auto table = copy->getSharedState()->fTable;
        auto scan = createFactorizedTableScanAligned(copyFrom->getOutExprs(), copyFrom->getSchema(),
            table, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(ops[0]));
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
    std::vector<std::string>& columnNames, logical_types_t& columnTypes) {
    for (auto& property : tableEntry->getPropertiesRef()) {
        columnNames.push_back(property.getName());
        columnTypes.push_back(property.getDataType()->copy());
    }
}

static void getRelColumnNamesInCopyOrder(TableCatalogEntry* tableEntry,
    std::vector<std::string>& columnNames, logical_types_t& columnTypes) {
    columnNames.emplace_back(InternalKeyword::SRC_OFFSET);
    columnNames.emplace_back(InternalKeyword::DST_OFFSET);
    columnNames.emplace_back(InternalKeyword::ROW_OFFSET);
    columnTypes.emplace_back(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    columnTypes.emplace_back(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    columnTypes.emplace_back(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    auto& properties = tableEntry->getPropertiesRef();
    for (auto i = 1u; i < properties.size(); ++i) { // skip internal ID
        columnNames.push_back(properties[i].getName());
        columnTypes.push_back(properties[i].getDataType()->copy());
    }
}

static std::shared_ptr<Expression> matchColumnExpression(
    const expression_vector& columnExpressions, const std::string& columnName) {
    for (auto& expression : columnExpressions) {
        KU_ASSERT(expression->expressionType == ExpressionType::VARIABLE);
        auto var = ku_dynamic_cast<Expression*, VariableExpression*>(expression.get());
        if (columnName == var->getVariableName()) {
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
    auto copyFrom = ku_dynamic_cast<LogicalOperator*, LogicalCopyFrom*>(logicalOperator);
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto nodeTableEntry =
        ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(copyFromInfo->tableEntry);
    // Map reader.
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    auto sharedState = std::make_shared<CopyNodeSharedState>();
    if (prevOperator->getOperatorType() == PhysicalOperatorType::IN_QUERY_CALL) {
        auto inQueryCall = ku_dynamic_cast<PhysicalOperator*, InQueryCall*>(prevOperator.get());
        sharedState->readerSharedState = inQueryCall->getSharedState();
    } else {
        KU_ASSERT(prevOperator->getOperatorType() == PhysicalOperatorType::AGGREGATE_SCAN);
        auto hashScan = ku_dynamic_cast<PhysicalOperator*, HashAggregateScan*>(prevOperator.get());
        sharedState->distinctSharedState = hashScan->getSharedState().get();
    }
    // Map copy node.
    auto nodeTable = storageManager.getNodeTable(nodeTableEntry->getTableID());
    sharedState->wal = storageManager.getWAL();
    sharedState->table = nodeTable;
    auto pk = nodeTableEntry->getPrimaryKey();
    sharedState->pkColumnIdx = nodeTableEntry->getColumnID(pk->getPropertyID());
    sharedState->pkType = *pk->getDataType();
    sharedState->fTable = getSingleStringColumnFTable();
    std::vector<std::string> columnNames;
    logical_types_t columnTypes;
    getNodeColumnsInCopyOrder(nodeTableEntry, columnNames, columnTypes);
    std::vector<std::string> columnNamesExcludingSerial;
    for (auto i = 0u; i < columnNames.size(); ++i) {
        if (columnTypes[i]->getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            continue;
        }
        columnNamesExcludingSerial.push_back(columnNames[i]);
    }
    auto inputColumns = copyFromInfo->fileScanInfo->columns;
    inputColumns.push_back(copyFromInfo->fileScanInfo->offset);
    std::unordered_set<storage::RelTable*> fwdRelTables;
    std::unordered_set<storage::RelTable*> bwdRelTables;
    for (auto relTableID : nodeTableEntry->getFwdRelTableIDSet()) {
        fwdRelTables.insert(storageManager.getRelTable(relTableID));
    }
    for (auto relTableID : nodeTableEntry->getBwdRelTableIDSet()) {
        bwdRelTables.insert(storageManager.getRelTable(relTableID));
    }
    auto columnPositions =
        getColumnDataPositions(columnNamesExcludingSerial, inputColumns, *outFSchema);
    sharedState->columnTypes = std::move(columnTypes);
    auto info = std::make_unique<CopyNodeInfo>(std::move(columnPositions), nodeTable, fwdRelTables,
        bwdRelTables, nodeTableEntry->getName(), copyFromInfo->containsSerial,
        storageManager.compressionEnabled());
    return std::make_unique<CopyNode>(sharedState, std::move(info),
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
        logical_types_t columnTypes;
        getRelColumnNamesInCopyOrder(info->tableEntry, columnNames, columnTypes);
        auto columnPositions = getColumnDataPositions(columnNames, info->payloads, *outFSchema);
        infos.push_back(std::make_unique<PartitioningInfo>(keyPos, columnPositions,
            std::move(columnTypes), PartitionerFunctions::partitionRelData));
    }
    auto sharedState = std::make_shared<PartitionerSharedState>(memoryManager);
    return std::make_unique<Partitioner>(std::make_unique<ResultSetDescriptor>(outFSchema),
        std::move(infos), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        logicalPartitioner->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::createCopyRel(
    std::shared_ptr<PartitionerSharedState> partitionerSharedState,
    std::shared_ptr<CopyRelSharedState> sharedState, LogicalCopyFrom* copyFrom,
    RelDataDirection direction) {
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto relTableEntry =
        ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(copyFromInfo->tableEntry);
    auto partitioningIdx = direction == RelDataDirection::FWD ? 0 : 1;
    auto dataFormat = relTableEntry->isSingleMultiplicity(direction) ? ColumnDataFormat::REGULAR :
                                                                       ColumnDataFormat::CSR;
    auto copyRelInfo = std::make_unique<CopyRelInfo>(relTableEntry, partitioningIdx, direction,
        dataFormat, storageManager.getWAL(), storageManager.compressionEnabled());
    return std::make_unique<CopyRel>(std::move(copyRelInfo), std::move(partitionerSharedState),
        std::move(sharedState), std::make_unique<ResultSetDescriptor>(outFSchema), getOperatorID(),
        copyFrom->getExpressionsForPrinting());
}

physical_op_vector_t PlanMapper::mapCopyRelFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto relTableEntry =
        ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(copyFromInfo->tableEntry);
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    KU_ASSERT(prevOperator->getOperatorType() == PhysicalOperatorType::PARTITIONER);
    auto partitionerSharedState = dynamic_cast<Partitioner*>(prevOperator.get())->getSharedState();
    partitionerSharedState->srcNodeTable =
        storageManager.getNodeTable(relTableEntry->getSrcTableID());
    partitionerSharedState->dstNodeTable =
        storageManager.getNodeTable(relTableEntry->getDstTableID());
    // TODO(Xiyang): Move binding of column types to binder.
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    columnTypes.push_back(LogicalType::INTERNAL_ID()); // ADJ COLUMN.
    for (auto& property : relTableEntry->getPropertiesRef()) {
        columnTypes.push_back(property.getDataType()->copy());
    }
    auto copyRelSharedState = std::make_shared<CopyRelSharedState>(relTableEntry->getTableID(),
        storageManager.getRelTable(relTableEntry->getTableID()), std::move(columnTypes),
        storageManager.getRelsStatistics(), memoryManager);
    auto copyRelFWD =
        createCopyRel(partitionerSharedState, copyRelSharedState, copyFrom, RelDataDirection::FWD);
    auto copyRelBWD =
        createCopyRel(partitionerSharedState, copyRelSharedState, copyFrom, RelDataDirection::BWD);
    physical_op_vector_t result;
    result.push_back(std::move(copyRelBWD));
    result.push_back(std::move(copyRelFWD));
    result.push_back(std::move(prevOperator));
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
    KU_ASSERT(rChild->getOperatorType() == PhysicalOperatorType::COPY_NODE);
    auto rCopy = ku_dynamic_cast<PhysicalOperator*, CopyNode*>(rChild.get());
    auto lChild = mapCopyNodeFrom(logicalLChild);
    auto lCopy = ku_dynamic_cast<PhysicalOperator*, CopyNode*>(lChild.get());
    auto rrrChildren = mapCopyRelFrom(logicalRRRChild);
    KU_ASSERT(rrrChildren[2]->getOperatorType() == PhysicalOperatorType::PARTITIONER);
    auto rrrPartitioner = ku_dynamic_cast<PhysicalOperator*, Partitioner*>(rrrChildren[2].get());
    rrrPartitioner->getSharedState()->copyNodeSharedStates.push_back(rCopy->getSharedState());
    rrrPartitioner->getSharedState()->copyNodeSharedStates.push_back(rCopy->getSharedState());
    KU_ASSERT(rrrChildren[2]->getChild(0)->getOperatorType() == PhysicalOperatorType::INDEX_LOOKUP);
    auto rrrLookup = ku_dynamic_cast<PhysicalOperator*, IndexLookup*>(rrrChildren[2]->getChild(0));
    rrrLookup->setCopyNodeSharedState(rCopy->getSharedState());
    auto rrlChildren = mapCopyRelFrom(logicalRRLChild);
    auto rrLPartitioner = ku_dynamic_cast<PhysicalOperator*, Partitioner*>(rrlChildren[2].get());
    rrLPartitioner->getSharedState()->copyNodeSharedStates.push_back(rCopy->getSharedState());
    rrLPartitioner->getSharedState()->copyNodeSharedStates.push_back(lCopy->getSharedState());
    KU_ASSERT(rrlChildren[2]->getChild(0)->getOperatorType() == PhysicalOperatorType::INDEX_LOOKUP);
    auto rrlLookup = ku_dynamic_cast<PhysicalOperator*, IndexLookup*>(rrlChildren[2]->getChild(0));
    rrlLookup->setCopyNodeSharedState(rCopy->getSharedState());
    auto sharedState = std::make_shared<CopyRdfSharedState>();
    auto fTable = getSingleStringColumnFTable();
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
    return createFactorizedTableScanAligned(copyFrom->getOutExprs(), copyFrom->getSchema(), fTable,
        DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(copyRdf));
}

} // namespace processor
} // namespace kuzu
