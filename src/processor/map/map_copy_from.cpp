#include "binder/copy/bound_copy_from.h"
#include "binder/expression/variable_expression.h"
#include "catalog/node_table_schema.h"
#include "planner/operator/logical_partitioner.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/copy_node.h"
#include "processor/operator/persistent/copy_rdf_resource.h"
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
    switch (copyFrom->getInfo()->tableSchema->getTableType()) {
    case TableType::NODE:
        return mapCopyNodeFrom(logicalOperator);
    case TableType::REL:
        return mapCopyRelFrom(logicalOperator);
        // LCOV_EXCL_START
    default:
        KU_UNREACHABLE;
    }
    // LCOV_EXCL_STOP
}

static void getNodeColumnsInCopyOrder(
    TableSchema* tableSchema, std::vector<std::string>& columnNames, logical_types_t& columnTypes) {
    for (auto& property : tableSchema->getProperties()) {
        columnNames.push_back(property->getName());
        columnTypes.push_back(property->getDataType()->copy());
    }
}

static void getRelColumnNamesInCopyOrder(
    TableSchema* tableSchema, std::vector<std::string>& columnNames, logical_types_t& columnTypes) {
    columnNames.emplace_back(InternalKeyword::SRC_OFFSET);
    columnNames.emplace_back(InternalKeyword::DST_OFFSET);
    columnNames.emplace_back(InternalKeyword::ROW_OFFSET);
    columnTypes.emplace_back(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    columnTypes.emplace_back(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    columnTypes.emplace_back(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    auto properties = tableSchema->getProperties();
    for (auto i = 1; i < properties.size(); ++i) { // skip internal ID
        columnNames.push_back(properties[i]->getName());
        columnTypes.push_back(properties[i]->getDataType()->copy());
    }
}

static std::shared_ptr<Expression> matchColumnExpression(
    const expression_vector& columnExpressions, const std::string& columnName) {
    for (auto& expression : columnExpressions) {
        KU_ASSERT(expression->expressionType == ExpressionType::VARIABLE);
        auto var = reinterpret_cast<binder::VariableExpression*>(expression.get());
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
    auto copyFrom = reinterpret_cast<LogicalCopyFrom*>(logicalOperator);
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = (catalog::NodeTableSchema*)copyFromInfo->tableSchema;
    // Map reader.
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    auto inQueryCall = reinterpret_cast<InQueryCall*>(prevOperator.get());
    // Map copy node.
    auto nodeTable = storageManager.getNodeTable(tableSchema->tableID);
    auto sharedState = std::make_shared<CopyNodeSharedState>(inQueryCall->getSharedState());
    sharedState->wal = storageManager.getWAL();
    sharedState->table = nodeTable;
    auto pk = tableSchema->getPrimaryKey();
    sharedState->pkColumnIdx = tableSchema->getColumnID(pk->getPropertyID());
    sharedState->pkType = pk->getDataType()->copy();
    sharedState->fTable = getSingleStringColumnFTable();
    std::vector<std::string> columnNames;
    logical_types_t columnTypes;
    getNodeColumnsInCopyOrder(tableSchema, columnNames, columnTypes);
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
    for (auto relTableID : tableSchema->getFwdRelTableIDSet()) {
        fwdRelTables.insert(storageManager.getRelTable(relTableID));
    }
    for (auto relTableID : tableSchema->getBwdRelTableIDSet()) {
        bwdRelTables.insert(storageManager.getRelTable(relTableID));
    }
    auto columnPositions =
        getColumnDataPositions(columnNamesExcludingSerial, inputColumns, *outFSchema);
    sharedState->columnTypes = std::move(columnTypes);
    auto info = std::make_unique<CopyNodeInfo>(std::move(columnPositions), nodeTable, fwdRelTables,
        bwdRelTables, tableSchema->tableName, copyFromInfo->containsSerial,
        storageManager.compressionEnabled());
    std::unique_ptr<PhysicalOperator> copyNode;
    auto readerConfig =
        reinterpret_cast<function::ScanBindData*>(copyFromInfo->fileScanInfo->bindData.get())
            ->config;
    if (readerConfig.fileType == FileType::TURTLE &&
        readerConfig.rdfReaderConfig->mode == RdfReaderMode::RESOURCE) {
        copyNode = std::make_unique<CopyRdfResource>(sharedState, std::move(info),
            std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    } else {
        copyNode = std::make_unique<CopyNode>(sharedState, std::move(info),
            std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    }
    auto outputExpressions = binder::expression_vector{copyFrom->getOutputExpression()->copy()};
    return createFactorizedTableScanAligned(outputExpressions, copyFrom->getSchema(),
        sharedState->fTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(copyNode));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapPartitioner(LogicalOperator* logicalOperator) {
    auto logicalPartitioner = reinterpret_cast<LogicalPartitioner*>(logicalOperator);
    auto prevOperator = mapOperator(logicalPartitioner->getChild(0).get());
    auto outFSchema = logicalPartitioner->getSchema();
    std::vector<std::unique_ptr<PartitioningInfo>> infos;
    infos.reserve(logicalPartitioner->getNumInfos());
    for (auto i = 0u; i < logicalPartitioner->getNumInfos(); i++) {
        auto info = logicalPartitioner->getInfo(i);
        auto keyPos = getDataPos(*info->key, *outFSchema);
        std::vector<std::string> columnNames;
        logical_types_t columnTypes;
        getRelColumnNamesInCopyOrder(info->tableSchema, columnNames, columnTypes);
        auto columnPositions = getColumnDataPositions(columnNames, info->payloads, *outFSchema);
        infos.push_back(std::make_unique<PartitioningInfo>(keyPos, columnPositions,
            std::move(columnTypes), PartitionerFunctions::partitionRelData));
    }
    auto sharedState = std::make_shared<PartitionerSharedState>();
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
    auto tableSchema = dynamic_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto partitioningIdx = direction == RelDataDirection::FWD ? 0 : 1;
    auto dataFormat = tableSchema->isSingleMultiplicityInDirection(direction) ?
                          ColumnDataFormat::REGULAR :
                          ColumnDataFormat::CSR;
    auto copyRelInfo = std::make_unique<CopyRelInfo>(tableSchema, partitioningIdx, direction,
        dataFormat, storageManager.getWAL(), storageManager.compressionEnabled());
    return std::make_unique<CopyRel>(std::move(copyRelInfo), std::move(partitionerSharedState),
        std::move(sharedState), std::make_unique<ResultSetDescriptor>(outFSchema), getOperatorID(),
        copyFrom->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyRelFrom(
    planner::LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    KU_ASSERT(prevOperator->getOperatorType() == PhysicalOperatorType::PARTITIONER);
    auto nodesStats = storageManager.getNodesStatisticsAndDeletedIDs();
    auto partitionerSharedState = dynamic_cast<Partitioner*>(prevOperator.get())->getSharedState();
    partitionerSharedState->maxNodeOffsets.resize(2);
    partitionerSharedState->maxNodeOffsets[0] =
        nodesStats->getMaxNodeOffset(transaction::Transaction::getDummyReadOnlyTrx().get(),
            tableSchema->getBoundTableID(RelDataDirection::FWD));
    partitionerSharedState->maxNodeOffsets[1] =
        nodesStats->getMaxNodeOffset(transaction::Transaction::getDummyReadOnlyTrx().get(),
            tableSchema->getBoundTableID(RelDataDirection::BWD));
    // TODO(Xiyang): Move binding of column types to binder.
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    columnTypes.push_back(LogicalType::INTERNAL_ID()); // ADJ COLUMN.
    for (auto& property : tableSchema->properties) {
        columnTypes.push_back(property->getDataType()->copy());
    }
    auto copyRelSharedState = std::make_shared<CopyRelSharedState>(tableSchema->tableID,
        storageManager.getRelTable(tableSchema->tableID), std::move(columnTypes),
        storageManager.getRelsStatistics(), memoryManager);
    auto copyRelFWD =
        createCopyRel(partitionerSharedState, copyRelSharedState, copyFrom, RelDataDirection::FWD);
    auto copyRelBWD =
        createCopyRel(partitionerSharedState, copyRelSharedState, copyFrom, RelDataDirection::BWD);
    auto outputExpressions = expression_vector{copyFrom->getOutputExpression()->copy()};
    auto fTableScan = createFactorizedTableScanAligned(outputExpressions, outFSchema,
        copyRelSharedState->getFTable(), DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyRelBWD));
    // Pipelines are scheduled as the order: partitioner -> copyRelFWD -> copyRelBWD.
    fTableScan->addChild(std::move(copyRelFWD));
    fTableScan->addChild(std::move(prevOperator));
    return fTableScan;
}

} // namespace processor
} // namespace kuzu
