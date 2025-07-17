#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog_entry/function_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/hnsw_index_catalog_entry.h"
#include "function/built_in_function_utils.h"
#include "function/hnsw_index_functions.h"
#include "function/table/bind_data.h"
#include "index/hnsw_index_utils.h"
#include "index/hnsw_rel_batch_insert.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_table_function_call.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/batch_insert.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::processor;
using namespace kuzu::processor;

namespace kuzu {
namespace vector_extension {

CreateInMemHNSWSharedState::CreateInMemHNSWSharedState(const CreateHNSWIndexBindData& bindData)
    : SimpleTableFuncSharedState{bindData.numRows}, name{bindData.indexName},
      nodeTable{bindData.context->getStorageManager()
                    ->getTable(bindData.tableEntry->getTableID())
                    ->cast<storage::NodeTable>()},
      numNodes{bindData.numRows}, bindData{&bindData} {
    storage::IndexInfo dummyIndexInfo{"", "", bindData.tableEntry->getTableID(),
        {bindData.tableEntry->getColumnID(bindData.propertyID)}, {PhysicalTypeID::ARRAY}, false,
        false};
    hnswIndex = std::make_shared<InMemHNSWIndex>(bindData.context, dummyIndexInfo,
        std::make_unique<storage::IndexStorageInfo>(), nodeTable,
        bindData.tableEntry->getColumnID(bindData.propertyID), bindData.config.copy());
}

static std::unique_ptr<TableFuncBindData> createInMemHNSWBindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto tableName = input->getLiteralVal<std::string>(0);
    const auto indexName = input->getLiteralVal<std::string>(1);
    const auto columnName = input->getLiteralVal<std::string>(2);
    auto tableEntry = HNSWIndexUtils::bindNodeTable(*context, tableName, indexName,
        HNSWIndexUtils::IndexOperation::CREATE);
    const auto tableID = tableEntry->getTableID();
    HNSWIndexUtils::validateColumnType(*tableEntry, columnName);
    const auto& table = context->getStorageManager()->getTable(tableID)->cast<storage::NodeTable>();
    auto propertyID = tableEntry->getPropertyID(columnName);
    auto config = HNSWIndexConfig{input->optionalParams};
    auto numNodes = table.getStats(context->getTransaction()).getTableCard();
    return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, propertyID,
        numNodes, std::move(config));
}

static std::unique_ptr<TableFuncSharedState> initCreateInMemHNSWSharedState(
    const TableFuncInitSharedStateInput& input) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    return std::make_unique<CreateInMemHNSWSharedState>(*bindData);
}

static std::unique_ptr<TableFuncLocalState> initCreateInMemHNSWLocalState(
    const TableFuncInitLocalStateInput& input) {
    const auto* bindData = input.bindData.constPtrCast<CreateHNSWIndexBindData>();
    const auto& index = input.sharedState.ptrCast<CreateInMemHNSWSharedState>()->hnswIndex;
    return std::make_unique<CreateInMemHNSWLocalState>(bindData->numRows + 1,
        index->getNumUpperLayerNodes(), index->constructEmbeddingsScanState());
}

static offset_t createInMemHNSWTableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto sharedState = input.sharedState->ptrCast<CreateInMemHNSWSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    const auto& hnswIndex = sharedState->hnswIndex;
    offset_t numNodesInserted = 0;
    for (auto i = morsel.startOffset; i < morsel.endOffset; i++) {
        numNodesInserted +=
            hnswIndex->insert(i, input.localState->ptrCast<CreateInMemHNSWLocalState>());
    }
    sharedState->numNodesInserted.fetch_add(numNodesInserted);
    return morsel.endOffset - morsel.startOffset;
}

static double createInMemHNSWProgressFunc(TableFuncSharedState* sharedState) {
    const auto hnswSharedState = sharedState->ptrCast<CreateInMemHNSWSharedState>();
    const auto numNodesInserted = hnswSharedState->numNodesInserted.load();
    if (hnswSharedState->numNodes == 0) {
        return 1.0;
    }
    if (numNodesInserted == 0) {
        return 0.0;
    }
    return static_cast<double>(numNodesInserted) / hnswSharedState->numNodes;
}

static std::unique_ptr<PhysicalOperator> getPhysicalPlan(PlanMapper* planMapper,
    const planner::LogicalOperator* logicalOp) {
    // _CreateHNSWIndex table function.
    auto logicalCallBoundData = logicalOp->constPtrCast<planner::LogicalTableFunctionCall>()
                                    ->getBindData()
                                    ->constPtrCast<CreateHNSWIndexBindData>();
    auto createHNSWCallOp = TableFunction::getPhysicalPlan(planMapper, logicalOp);
    KU_ASSERT(createHNSWCallOp->getOperatorType() == PhysicalOperatorType::TABLE_FUNCTION_CALL);
    auto createFuncCall = createHNSWCallOp->ptrCast<TableFunctionCall>();
    auto createFuncSharedState =
        createFuncCall->getSharedState()->ptrCast<CreateInMemHNSWSharedState>();
    auto indexName = createFuncSharedState->name;
    // Append a dummy sink to end the first pipeline
    auto createDummySink =
        std::make_unique<DummySink>(std::move(createHNSWCallOp), planMapper->getOperatorID());
    createDummySink->setDescriptor(std::make_unique<ResultSetDescriptor>(logicalOp->getSchema()));
    // Append _FinalizeHNSWIndex table function.
    auto clientContext = planMapper->clientContext;
    auto finalizeFuncEntry =
        clientContext->getCatalog()->getFunctionEntry(clientContext->getTransaction(),
            InternalFinalizeHNSWIndexFunction::name, true /* useInternal */);
    auto func = BuiltInFunctionsUtils::matchFunction(InternalFinalizeHNSWIndexFunction::name,
        finalizeFuncEntry->ptrCast<catalog::FunctionCatalogEntry>());
    auto info = TableFunctionCallInfo();
    info.function = *func->constPtrCast<TableFunction>();
    info.bindData = std::make_unique<TableFuncBindData>();
    auto initInput =
        TableFuncInitSharedStateInput(info.bindData.get(), planMapper->executionContext);
    auto sharedState = info.function.initSharedStateFunc(initInput);
    auto finalizeFuncSharedState = sharedState->ptrCast<FinalizeHNSWSharedState>();
    finalizeFuncSharedState->hnswIndex = createFuncSharedState->hnswIndex;
    auto numNodeGroups =
        logicalCallBoundData->numRows == 0 ?
            0 :
            storage::StorageUtils::getNodeGroupIdx(logicalCallBoundData->numRows) + 1;
    finalizeFuncSharedState->numRows = numNodeGroups;
    finalizeFuncSharedState->maxMorselSize = 1;
    finalizeFuncSharedState->bindData = logicalCallBoundData->copy();
    auto finalizeCallOp = std::make_unique<TableFunctionCall>(std::move(info), sharedState,
        planMapper->getOperatorID(), OPPrintInfo::EmptyInfo());
    finalizeCallOp->addChild(std::move(createDummySink));
    // Append a dummy sink to the end of the second pipeline
    auto finalizeHNSWDummySink =
        std::make_unique<DummySink>(std::move(finalizeCallOp), planMapper->getOperatorID());
    finalizeHNSWDummySink->setDescriptor(
        std::make_unique<ResultSetDescriptor>(logicalOp->getSchema()));
    // Append RelBatchInsert pipelines.
    // Get tables from storage.
    const auto storageManager = clientContext->getStorageManager();
    const auto catalog = clientContext->getCatalog();
    const auto transaction = clientContext->getTransaction();
    auto nodeTable = storageManager->getTable(logicalCallBoundData->tableEntry->getTableID())
                         ->ptrCast<storage::NodeTable>();
    auto nodeTableID = nodeTable->getTableID();
    auto upperRelTableName = HNSWIndexUtils::getUpperGraphTableName(nodeTableID, indexName);
    auto upperRelTableEntry = catalog->getTableCatalogEntry(transaction, upperRelTableName)
                                  ->ptrCast<catalog::RelGroupCatalogEntry>();
    auto upperRelTable = storageManager->getTable(upperRelTableEntry->getSingleRelEntryInfo().oid)
                             ->ptrCast<storage::RelTable>();
    auto lowerRelTableName = HNSWIndexUtils::getLowerGraphTableName(nodeTableID, indexName);
    auto lowerRelTableEntry = catalog->getTableCatalogEntry(transaction, lowerRelTableName)
                                  ->ptrCast<catalog::RelGroupCatalogEntry>();
    auto lowerRelTable = storageManager->getTable(lowerRelTableEntry->getSingleRelEntryInfo().oid)
                             ->ptrCast<storage::RelTable>();
    // Initialize partitioner shared state.
    auto& partitionerSharedState = finalizeFuncSharedState->partitionerSharedState;
    partitionerSharedState->setTables(nodeTable, upperRelTable, lowerRelTable);
    logical_type_vec_t callColumnTypes;
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    finalizeFuncSharedState->partitionerSharedState->initialize(callColumnTypes, clientContext);
    // Initialize fTable for BatchInsert.
    auto fTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    // Create RelBatchInsert and dummy sink operators.
    const auto upperBatchInsertSharedState = std::make_shared<BatchInsertSharedState>(fTable);
    auto upperInsertInfo = std::make_unique<RelBatchInsertInfo>(upperRelTableEntry->getName(),
        std::vector<LogicalType>{} /* warningColumnTypes */, nodeTableID /* fromTableID */,
        nodeTableID /* toTableID */, RelDataDirection::FWD);
    auto upperPrintInfo = std::make_unique<RelBatchInsertPrintInfo>(upperRelTableEntry->getName());
    auto upperProgress = std::make_shared<RelBatchInsertProgressSharedState>();
    auto upperBatchInsert = std::make_unique<RelBatchInsert>(std::move(upperInsertInfo),
        partitionerSharedState->upperPartitionerSharedState, upperBatchInsertSharedState,
        planMapper->getOperatorID(), std::move(upperPrintInfo), upperProgress,
        std::make_unique<HNSWRelBatchInsert>());
    upperBatchInsert->setDescriptor(std::make_unique<ResultSetDescriptor>(logicalOp->getSchema()));
    const auto lowerBatchInsertSharedState = std::make_shared<BatchInsertSharedState>(fTable);
    auto lowerInsertInfo = std::make_unique<RelBatchInsertInfo>(lowerRelTableEntry->getName(),
        std::vector<LogicalType>{} /* warningColumnTypes */, nodeTableID /* fromTableID */,
        nodeTableID /* toTableID */, RelDataDirection::FWD);
    auto lowerPrintInfo = std::make_unique<RelBatchInsertPrintInfo>(lowerRelTableEntry->getName());
    auto lowerProgress = std::make_shared<RelBatchInsertProgressSharedState>();
    auto lowerBatchInsert = std::make_unique<RelBatchInsert>(std::move(lowerInsertInfo),
        partitionerSharedState->lowerPartitionerSharedState, lowerBatchInsertSharedState,
        planMapper->getOperatorID(), std::move(lowerPrintInfo), lowerProgress,
        std::make_unique<HNSWRelBatchInsert>());
    lowerBatchInsert->setDescriptor(std::make_unique<ResultSetDescriptor>(logicalOp->getSchema()));
    physical_op_vector_t children;
    children.push_back(std::move(upperBatchInsert));
    children.push_back(std::move(lowerBatchInsert));
    children.push_back(std::move(finalizeHNSWDummySink));
    return planMapper->createFTableScanAligned({}, logicalOp->getSchema(), fTable,
        DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(children));
}

function_set InternalCreateHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->bindFunc = createInMemHNSWBindFunc;
    func->initSharedStateFunc = initCreateInMemHNSWSharedState;
    func->initLocalStateFunc = initCreateInMemHNSWLocalState;
    func->tableFunc = createInMemHNSWTableFunc;
    func->canParallelFunc = [] { return true; };
    func->progressFunc = createInMemHNSWProgressFunc;
    func->getPhysicalPlanFunc = getPhysicalPlan;
    func->isReadOnly = false;
    functionSet.push_back(std::move(func));
    return functionSet;
}

static std::unique_ptr<TableFuncBindData> finalizeHNSWBindFunc(main::ClientContext*,
    const TableFuncBindInput*) {
    return std::make_unique<TableFuncBindData>();
}

static std::unique_ptr<TableFuncSharedState> initFinalizeHNSWSharedState(
    const TableFuncInitSharedStateInput&) {
    return std::make_unique<FinalizeHNSWSharedState>();
}

static offset_t finalizeHNSWTableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto sharedState = input.sharedState->ptrCast<FinalizeHNSWSharedState>();
    const auto& hnswIndex = input.sharedState->ptrCast<FinalizeHNSWSharedState>()->hnswIndex;
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    for (auto i = morsel.startOffset; i < morsel.endOffset; i++) {
        hnswIndex->finalizeNodeGroup(i);
    }
    sharedState->numNodeGroupsFinalized.fetch_add(morsel.endOffset - morsel.startOffset);
    return morsel.endOffset - morsel.startOffset;
}

static void finalizeHNSWTableFinalizeFunc(const ExecutionContext* context,
    TableFuncSharedState* sharedState) {
    const auto clientContext = context->clientContext;
    const auto transaction = clientContext->getTransaction();
    const auto hnswSharedState = sharedState->ptrCast<FinalizeHNSWSharedState>();
    const auto index = hnswSharedState->hnswIndex.get();
    const auto bindData = hnswSharedState->bindData->constPtrCast<CreateHNSWIndexBindData>();
    const auto catalog = clientContext->getCatalog();
    const auto nodeTableID = bindData->tableEntry->getTableID();
    auto& upperTableEntry =
        catalog
            ->getTableCatalogEntry(transaction,
                HNSWIndexUtils::getUpperGraphTableName(nodeTableID, bindData->indexName))
            ->cast<catalog::RelGroupCatalogEntry>();
    auto& lowerTableEntry =
        catalog
            ->getTableCatalogEntry(transaction,
                HNSWIndexUtils::getLowerGraphTableName(nodeTableID, bindData->indexName))
            ->cast<catalog::RelGroupCatalogEntry>();
    auto auxInfo = std::make_unique<HNSWIndexAuxInfo>(bindData->config.copy());
    auto indexEntry = std::make_unique<catalog::IndexCatalogEntry>(HNSWIndexCatalogEntry::TYPE_NAME,
        bindData->tableEntry->getTableID(), bindData->indexName, std::vector{bindData->propertyID},
        std::move(auxInfo));
    catalog->createIndex(transaction, std::move(indexEntry));
    auto columnID = bindData->tableEntry->getColumnID(bindData->propertyID);
    const auto hnswIndexType = OnDiskHNSWIndex::getIndexType();
    storage::IndexInfo indexInfo{bindData->indexName, hnswIndexType.typeName, nodeTableID,
        {columnID}, {PhysicalTypeID::ARRAY},
        hnswIndexType.constraintType == storage::IndexConstraintType::PRIMARY,
        hnswIndexType.definitionType == storage::IndexDefinitionType::BUILTIN};
    auto upperTableID = upperTableEntry.getSingleRelEntryInfo().oid;
    auto lowerTableID = lowerTableEntry.getSingleRelEntryInfo().oid;
    auto storageInfo = std::make_unique<HNSWStorageInfo>(upperTableID, lowerTableID,
        index->getUpperEntryPoint(), index->getLowerEntryPoint(), bindData->numRows);
    auto onDiskIndex = std::make_unique<OnDiskHNSWIndex>(context->clientContext, indexInfo,
        std::move(storageInfo), bindData->config.copy());
    auto storageManager = clientContext->getStorageManager();
    auto nodeTable = storageManager->getTable(nodeTableID)->ptrCast<storage::NodeTable>();
    nodeTable->addIndex(std::move(onDiskIndex));
    index->moveToPartitionState(*hnswSharedState->partitionerSharedState);
    transaction->setForceCheckpoint();
}

static double finalizeHNSWProgressFunc(TableFuncSharedState* sharedState) {
    const auto hnswSharedState = sharedState->ptrCast<FinalizeHNSWSharedState>();
    const auto numNodeGroupsFinalized = hnswSharedState->numNodeGroupsFinalized.load();
    if (hnswSharedState->numRows == 0) {
        return 1.0;
    }
    if (numNodeGroupsFinalized == 0) {
        return 0.0;
    }
    return static_cast<double>(numNodeGroupsFinalized) / hnswSharedState->numRows;
}

function_set InternalFinalizeHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector<LogicalTypeID> inputTypes = {};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->bindFunc = finalizeHNSWBindFunc;
    func->initSharedStateFunc = initFinalizeHNSWSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->tableFunc = finalizeHNSWTableFunc;
    func->finalizeFunc = finalizeHNSWTableFinalizeFunc;
    func->progressFunc = finalizeHNSWProgressFunc;
    func->canParallelFunc = [] { return true; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    HNSWIndexUtils::validateAutoTransaction(*context, CreateVectorIndexFunction::name);
    return createInMemHNSWBindFunc(context, input);
}

static std::string rewriteCreateHNSWQuery(main::ClientContext& context,
    const TableFuncBindData& bindData) {
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    const auto hnswBindData = bindData.constPtrCast<CreateHNSWIndexBindData>();
    std::string query = "BEGIN TRANSACTION;";
    auto indexName = hnswBindData->indexName;
    auto tableName = hnswBindData->tableEntry->getName();
    auto tableID = hnswBindData->tableEntry->getTableID();
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {}) WITH (storage_direction='fwd');",
        HNSWIndexUtils::getUpperGraphTableName(tableID, indexName), tableName, tableName);
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {}) WITH (storage_direction='fwd');",
        HNSWIndexUtils::getLowerGraphTableName(tableID, indexName), tableName, tableName);
    std::string params;
    auto& config = hnswBindData->config;
    params += stringFormat("mu := {}, ", config.mu);
    params += stringFormat("ml := {}, ", config.ml);
    params += stringFormat("efc := {}, ", config.efc);
    params += stringFormat("metric := '{}', ", HNSWIndexConfig::metricToString(config.metric));
    params += stringFormat("alpha := {}, ", config.alpha);
    params += stringFormat("pu := {}, ", config.pu);
    params +=
        stringFormat("cache_embeddings := {}", config.cacheEmbeddingsColumn ? "true" : "false");
    auto columnName = hnswBindData->tableEntry->getProperty(hnswBindData->propertyID).getName();
    if (config.cacheEmbeddingsColumn) {
        query +=
            stringFormat("CALL _CACHE_ARRAY_COLUMN_LOCALLY('{}', '{}');", tableName, columnName);
    }
    query += stringFormat("CALL _CREATE_HNSW_INDEX('{}', '{}', '{}', {});", tableName, indexName,
        columnName, params);
    query += stringFormat("RETURN 'Index {} has been created.' as result;", indexName);
    query += "COMMIT;";
    return query;
}

function_set CreateVectorIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->tableFunc = TableFunction::emptyTableFunc;
    func->rewriteFunc = rewriteCreateHNSWQuery;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace vector_extension
} // namespace kuzu
