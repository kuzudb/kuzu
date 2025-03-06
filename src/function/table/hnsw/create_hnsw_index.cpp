#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "function/built_in_function_utils.h"
#include "function/table/bind_data.h"
#include "function/table/hnsw/hnsw_index_functions.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_table_function_call.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/batch_insert.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/index/index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

CreateInMemHNSWSharedState::CreateInMemHNSWSharedState(const CreateHNSWIndexBindData& bindData)
    : SimpleTableFuncSharedState{bindData.numRows}, name{bindData.indexName},
      nodeTable{bindData.context->getStorageManager()
                    ->getTable(bindData.tableEntry->getTableID())
                    ->cast<storage::NodeTable>()},
      numNodes{bindData.numRows}, bindData{&bindData} {
    hnswIndex = std::make_shared<storage::InMemHNSWIndex>(bindData.context, nodeTable,
        bindData.tableEntry->getColumnID(bindData.propertyID), bindData.config.copy());
}

static std::unique_ptr<TableFuncBindData> createInMemHNSWBindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto tableName = input->getLiteralVal<std::string>(0);
    const auto indexName = input->getLiteralVal<std::string>(1);
    const auto columnName = input->getLiteralVal<std::string>(2);
    auto tableEntry = storage::IndexUtils::bindNodeTable(*context, tableName, indexName,
        storage::IndexOperation::CREATE);
    const auto tableID = tableEntry->getTableID();
    storage::HNSWIndexUtils::validateColumnType(*tableEntry, columnName);
    const auto& table = context->getStorageManager()->getTable(tableID)->cast<storage::NodeTable>();
    auto propertyID = tableEntry->getPropertyID(columnName);
    auto config = storage::HNSWIndexConfig{input->optionalParams};
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
    const auto bindData = input.bindData.constPtrCast<CreateHNSWIndexBindData>();
    return std::make_unique<CreateInMemHNSWLocalState>(bindData->numRows + 1);
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
        numNodesInserted += hnswIndex->insert(i,
            input.localState->ptrCast<CreateInMemHNSWLocalState>()->upperVisited,
            input.localState->ptrCast<CreateInMemHNSWLocalState>()->lowerVisited);
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

static std::unique_ptr<processor::PhysicalOperator> getPhysicalPlan(
    processor::PlanMapper* planMapper, const planner::LogicalOperator* logicalOp) {
    // _CreateHNSWIndex table function.
    auto logicalCallBoundData = logicalOp->constPtrCast<planner::LogicalTableFunctionCall>()
                                    ->getBindData()
                                    ->constPtrCast<CreateHNSWIndexBindData>();
    auto createHNSWCallOp = TableFunction::getPhysicalPlan(planMapper, logicalOp);
    KU_ASSERT(createHNSWCallOp->getOperatorType() ==
              processor::PhysicalOperatorType::TABLE_FUNCTION_CALL);
    auto createFuncCall = createHNSWCallOp->ptrCast<processor::TableFunctionCall>();
    auto createFuncSharedState =
        createFuncCall->getSharedState()->ptrCast<CreateInMemHNSWSharedState>();
    auto indexName = createFuncSharedState->name;
    // Append a dummy sink to end the first pipeline
    auto createDummySink = std::make_unique<processor::DummySink>(
        std::make_unique<processor::ResultSetDescriptor>(logicalOp->getSchema()),
        std::move(createHNSWCallOp), planMapper->getOperatorID(), std::make_unique<OPPrintInfo>());
    // Append _FinalizeHNSWIndex table function.
    auto clientContext = planMapper->clientContext;
    auto finalizeFuncEntry = clientContext->getCatalog()->getFunctionEntry(
        clientContext->getTransaction(), InternalFinalizeHNSWIndexFunction::name);
    auto func = BuiltInFunctionsUtils::matchFunction(InternalFinalizeHNSWIndexFunction::name,
        finalizeFuncEntry->ptrCast<catalog::FunctionCatalogEntry>())
                    ->constPtrCast<TableFunction>();
    auto info = processor::TableFunctionCallInfo();
    info.function = *func;
    info.bindData = std::make_unique<TableFuncBindData>();
    auto initInput =
        TableFuncInitSharedStateInput(info.bindData.get(), planMapper->executionContext);
    auto sharedState = info.function.initSharedStateFunc(initInput);
    auto finalizeFuncSharedState = sharedState->ptrCast<FinalizeHNSWSharedState>();
    finalizeFuncSharedState->hnswIndex = createFuncSharedState->hnswIndex;
    finalizeFuncSharedState->numRows =
        (logicalCallBoundData->numRows + StorageConfig::NODE_GROUP_SIZE - 1) /
        StorageConfig::NODE_GROUP_SIZE;
    finalizeFuncSharedState->maxMorselSize = 1;
    finalizeFuncSharedState->bindData = logicalCallBoundData->copy();
    auto finalizeCallOp = std::make_unique<processor::TableFunctionCall>(std::move(info),
        sharedState, planMapper->getOperatorID(), std::make_unique<OPPrintInfo>());
    finalizeCallOp->addChild(std::move(createDummySink));
    // Append a dummy sink to the end of the second pipeline
    auto finalizeHNSWDummySink = std::make_unique<processor::DummySink>(
        std::make_unique<processor::ResultSetDescriptor>(logicalOp->getSchema()),
        std::move(finalizeCallOp), planMapper->getOperatorID(), std::make_unique<OPPrintInfo>());
    // Append RelBatchInsert pipelines.
    // Get tables from storage.
    const auto storageManager = clientContext->getStorageManager();
    auto nodeTable = storageManager->getTable(logicalCallBoundData->tableEntry->getTableID())
                         ->ptrCast<storage::NodeTable>();
    auto upperRelTableEntry =
        clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTransaction(),
            storage::HNSWIndexUtils::getUpperGraphTableName(indexName));
    auto upperRelTable =
        storageManager->getTable(upperRelTableEntry->getTableID())->ptrCast<storage::RelTable>();
    auto lowerRelTableEntry =
        clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTransaction(),
            storage::HNSWIndexUtils::getLowerGraphTableName(indexName));
    auto lowerRelTable =
        storageManager->getTable(lowerRelTableEntry->getTableID())->ptrCast<storage::RelTable>();
    // Initialize partitioner shared state.
    const auto partitionerSharedState = finalizeFuncSharedState->partitionerSharedState;
    partitionerSharedState->setTables(nodeTable, upperRelTable);
    logical_type_vec_t callColumnTypes;
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    finalizeFuncSharedState->partitionerSharedState->initialize(callColumnTypes, clientContext);
    // Initialize fTable for BatchInsert.
    auto fTable = processor::FactorizedTableUtils::getSingleStringColumnFTable(
        clientContext->getMemoryManager());
    // Figure out column types and IDs to COPY into.
    std::vector<LogicalType> columnTypes;
    std::vector<column_id_t> columnIDs;
    columnTypes.push_back(LogicalType::INTERNAL_ID()); // NBR_ID COLUMN.
    columnIDs.push_back(0);                            // NBR_ID COLUMN.
    for (auto& property : upperRelTableEntry->getProperties()) {
        columnTypes.push_back(property.getType().copy());
        columnIDs.push_back(upperRelTableEntry->getColumnID(property.getName()));
    }
    // Create RelBatchInsert and dummy sink operators.
    binder::BoundCopyFromInfo upperCopyFromInfo(upperRelTableEntry, nullptr, nullptr, {}, {},
        nullptr);
    const auto upperBatchInsertSharedState = std::make_shared<processor::BatchInsertSharedState>(
        upperRelTable, fTable, &storageManager->getWAL(), clientContext->getMemoryManager());
    auto copyRelUpper = planMapper->createRelBatchInsertOp(clientContext,
        partitionerSharedState->upperPartitionerSharedState, upperBatchInsertSharedState,
        upperCopyFromInfo, logicalOp->getSchema(), RelDataDirection::FWD, columnIDs,
        LogicalType::copy(columnTypes), planMapper->getOperatorID());
    binder::BoundCopyFromInfo lowerCopyFromInfo(lowerRelTableEntry, nullptr, nullptr, {}, {},
        nullptr);
    lowerCopyFromInfo.tableEntry = lowerRelTableEntry;
    const auto lowerBatchInsertSharedState = std::make_shared<processor::BatchInsertSharedState>(
        lowerRelTable, fTable, &storageManager->getWAL(), clientContext->getMemoryManager());
    auto copyRelLower = planMapper->createRelBatchInsertOp(clientContext,
        partitionerSharedState->lowerPartitionerSharedState, lowerBatchInsertSharedState,
        lowerCopyFromInfo, logicalOp->getSchema(), RelDataDirection::FWD, columnIDs,
        LogicalType::copy(columnTypes), planMapper->getOperatorID());
    processor::physical_op_vector_t children;
    children.push_back(std::move(copyRelUpper));
    children.push_back(std::move(copyRelLower));
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
    functionSet.push_back(std::move(func));
    return functionSet;
}

static std::unique_ptr<TableFuncBindData> finalizeHNSWBindFunc(main::ClientContext*,
    const TableFuncBindInput*) {
    return std::make_unique<TableFuncBindData>();
}

static std::unique_ptr<TableFuncSharedState> initFinalizeHNSWSharedState(
    const TableFuncInitSharedStateInput& input) {
    return std::make_unique<FinalizeHNSWSharedState>(
        *input.context->clientContext->getMemoryManager());
}

static offset_t finalizeHNSWTableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto& context = *input.context->clientContext;
    const auto sharedState = input.sharedState->ptrCast<FinalizeHNSWSharedState>();
    const auto& hnswIndex = input.sharedState->ptrCast<FinalizeHNSWSharedState>()->hnswIndex;
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    for (auto i = morsel.startOffset; i < morsel.endOffset; i++) {
        hnswIndex->finalize(*context.getMemoryManager(), i, *sharedState->partitionerSharedState);
    }
    sharedState->numNodeGroupsFinalized.fetch_add(morsel.endOffset - morsel.startOffset);
    return morsel.endOffset - morsel.startOffset;
}

static void finalizeHNSWTableFinalizeFunc(const processor::ExecutionContext* context,
    TableFuncSharedState* sharedState) {
    const auto clientContext = context->clientContext;
    const auto transaction = clientContext->getTransaction();
    const auto hnswSharedState = sharedState->ptrCast<FinalizeHNSWSharedState>();
    const auto index = hnswSharedState->hnswIndex.get();
    const auto bindData = hnswSharedState->bindData->constPtrCast<CreateHNSWIndexBindData>();
    const auto catalog = clientContext->getCatalog();
    auto upperRelTableID =
        catalog
            ->getTableCatalogEntry(transaction,
                storage::HNSWIndexUtils::getUpperGraphTableName(bindData->indexName))
            ->getTableID();
    auto lowerRelTableID =
        catalog
            ->getTableCatalogEntry(transaction,
                storage::HNSWIndexUtils::getLowerGraphTableName(bindData->indexName))
            ->getTableID();
    auto auxInfo = std::make_unique<catalog::HNSWIndexAuxInfo>(upperRelTableID, lowerRelTableID,
        index->getUpperEntryPoint(), index->getLowerEntryPoint(), bindData->config.copy());
    auto indexEntry = std::make_unique<catalog::IndexCatalogEntry>(
        catalog::HNSWIndexCatalogEntry::TYPE_NAME, bindData->tableEntry->getTableID(),
        bindData->indexName, std::vector{bindData->propertyID}, std::move(auxInfo));
    catalog->createIndex(transaction, std::move(indexEntry));
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
    storage::IndexUtils::validateAutoTransaction(*context, CreateHNSWIndexFunction::name);
    return createInMemHNSWBindFunc(context, input);
}

static std::string rewriteCreateHNSWQuery(main::ClientContext& context,
    const TableFuncBindData& bindData) {
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    const auto hnswBindData = bindData.constPtrCast<CreateHNSWIndexBindData>();
    std::string query = "BEGIN TRANSACTION;";
    auto indexName = hnswBindData->indexName;
    auto tableName = hnswBindData->tableEntry->getName();
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getUpperGraphTableName(indexName), tableName, tableName);
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getLowerGraphTableName(indexName), tableName, tableName);
    std::string params;
    auto& config = hnswBindData->config;
    params += stringFormat("mu := {}, ", config.mu);
    params += stringFormat("ml := {}, ", config.ml);
    params += stringFormat("efc := {}, ", config.efc);
    params += stringFormat("distFunc := '{}', ",
        storage::HNSWIndexConfig::distFuncToString(config.distFunc));
    params += stringFormat("alpha := {}, ", config.alpha);
    params += stringFormat("pu := {}", config.pu);
    auto columnName = hnswBindData->tableEntry->getProperty(hnswBindData->propertyID).getName();
    query += stringFormat("CALL _CACHE_ARRAY_COLUMN_LOCALLY('{}', '{}');", tableName, columnName);
    query += stringFormat("CALL _CREATE_HNSW_INDEX('{}', '{}', '{}', {});", tableName, indexName,
        columnName, params);
    query += stringFormat("RETURN 'Index {} has been created.' as result;", indexName);
    query += "COMMIT;";
    return query;
}

function_set CreateHNSWIndexFunction::getFunctionSet() {
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

} // namespace function
} // namespace kuzu
