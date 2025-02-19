#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
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

CreateHNSWSharedState::CreateHNSWSharedState(const CreateHNSWIndexBindData& bindData)
    : TableFuncSharedState{bindData.maxOffset}, name{bindData.indexName},
      nodeTable{bindData.context->getStorageManager()
                    ->getTable(bindData.tableEntry->getTableID())
                    ->cast<storage::NodeTable>()},
      numNodes{bindData.numNodes}, bindData{&bindData} {
    hnswIndex = std::make_unique<storage::InMemHNSWIndex>(bindData.context, nodeTable,
        bindData.tableEntry->getColumnID(bindData.propertyID), bindData.config.copy());
    partitionerSharedState = std::make_shared<storage::HNSWIndexPartitionerSharedState>(
        *bindData.context->getMemoryManager());
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    storage::IndexUtils::validateAutoTransaction(*context, CreateHNSWIndexFunction::name);
    const auto indexName = input->getLiteralVal<std::string>(0);
    const auto tableName = input->getLiteralVal<std::string>(1);
    const auto columnName = input->getLiteralVal<std::string>(2);
    auto tableEntry = storage::IndexUtils::bindTable(*context, tableName, indexName,
        storage::IndexOperation::CREATE);
    const auto tableID = tableEntry->getTableID();
    storage::HNSWIndexUtils::validateColumnType(*tableEntry, columnName);
    const auto& table = context->getStorageManager()->getTable(tableID)->cast<storage::NodeTable>();
    auto propertyID = tableEntry->getPropertyID(columnName);
    auto config = storage::HNSWIndexConfig{input->optionalParams};
    auto numNodes = table.getStats(context->getTransaction()).getTableCard();
    auto maxOffset = numNodes > 0 ? numNodes - 1 : 0;
    return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, propertyID,
        numNodes, maxOffset, std::move(config));
}

static std::unique_ptr<TableFuncSharedState> initCreateHNSWSharedState(
    const TableFunctionInitInput& input) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    return std::make_unique<CreateHNSWSharedState>(*bindData);
}

static std::unique_ptr<TableFuncLocalState> initCreateHNSWLocalState(
    const TableFunctionInitInput& input, TableFuncSharedState*, storage::MemoryManager*) {
    const auto bindData = input.bindData->constPtrCast<CreateHNSWIndexBindData>();
    return std::make_unique<CreateHNSWLocalState>(bindData->maxOffset + 1);
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto& context = *input.context->clientContext;
    const auto sharedState = input.sharedState->ptrCast<CreateHNSWSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (morsel.isInvalid()) {
        return 0;
    }
    const auto& hnswIndex = sharedState->hnswIndex;
    for (auto i = morsel.startOffset; i <= morsel.endOffset; i++) {
        hnswIndex->insert(i, context.getTransaction(),
            input.localState->ptrCast<CreateHNSWLocalState>()->upperVisited,
            input.localState->ptrCast<CreateHNSWLocalState>()->lowerVisited);
    }
    sharedState->numNodesInserted.fetch_add(morsel.endOffset - morsel.startOffset);
    return morsel.endOffset - morsel.startOffset;
}

static void finalizeFunc(const processor::ExecutionContext* context,
    TableFuncSharedState* sharedState) {
    const auto clientContext = context->clientContext;
    const auto transaction = clientContext->getTransaction();
    const auto hnswSharedState = sharedState->ptrCast<CreateHNSWSharedState>();
    const auto index = hnswSharedState->hnswIndex.get();
    index->shrink(transaction);
    index->finalize(*clientContext->getMemoryManager(), *hnswSharedState->partitionerSharedState);

    const auto bindData = hnswSharedState->bindData;
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

static std::string createHNSWIndexTables(main::ClientContext& context,
    const TableFuncBindData& bindData) {
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    const auto hnswBindData = bindData.constPtrCast<CreateHNSWIndexBindData>();
    std::string query = "";
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getUpperGraphTableName(hnswBindData->indexName),
        hnswBindData->tableEntry->getName(), hnswBindData->tableEntry->getName());
    query += stringFormat("CREATE REL TABLE {} (FROM {} TO {});",
        storage::HNSWIndexUtils::getLowerGraphTableName(hnswBindData->indexName),
        hnswBindData->tableEntry->getName(), hnswBindData->tableEntry->getName());
    std::string params;
    params += stringFormat("mu := {}, ", hnswBindData->config.mu);
    params += stringFormat("ml := {}, ", hnswBindData->config.ml);
    params += stringFormat("efc := {}, ", hnswBindData->config.efc);
    params += stringFormat("distFunc := '{}', ",
        storage::HNSWIndexConfig::distFuncToString(hnswBindData->config.distFunc));
    params += stringFormat("alpha := {}, ", hnswBindData->config.alpha);
    params += stringFormat("pu := {}", hnswBindData->config.pu);
    query += stringFormat("CALL _CREATE_HNSW_INDEX('{}', '{}', '{}', {});", hnswBindData->indexName,
        hnswBindData->tableEntry->getName(),
        hnswBindData->tableEntry->getProperty(hnswBindData->propertyID).getName(), params);
    query +=
        stringFormat("RETURN 'Index {} has been created.' as result;", hnswBindData->indexName);
    return query;
}

static double progressFunc(TableFuncSharedState* sharedState) {
    const auto hnswSharedState = sharedState->ptrCast<CreateHNSWSharedState>();
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
    const main::ClientContext* clientContext, processor::PlanMapper* planMapper,
    const planner::LogicalOperator* logicalOp) {
    auto logicalCallBoundData = logicalOp->constPtrCast<planner::LogicalTableFunctionCall>()
                                    ->getBindData()
                                    ->constPtrCast<CreateHNSWIndexBindData>();
    auto callOp = TableFunction::getPhysicalPlan(clientContext, planMapper, logicalOp);
    auto tableFuncCallOp = callOp->ptrCast<processor::TableFunctionCall>();
    KU_ASSERT(
        tableFuncCallOp->getOperatorType() == processor::PhysicalOperatorType::TABLE_FUNCTION_CALL);
    auto tableFuncSharedState =
        tableFuncCallOp->getSharedState()->funcState->ptrCast<CreateHNSWSharedState>();
    // Get tables from storage.
    const auto storageManager = clientContext->getStorageManager();
    auto nodeTable = storageManager->getTable(logicalCallBoundData->tableEntry->getTableID())
                         ->ptrCast<storage::NodeTable>();
    auto upperRelTableEntry =
        clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTransaction(),
            storage::HNSWIndexUtils::getUpperGraphTableName(tableFuncSharedState->name));
    auto upperRelTable =
        storageManager->getTable(upperRelTableEntry->getTableID())->ptrCast<storage::RelTable>();
    auto lowerRelTableEntry =
        clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTransaction(),
            storage::HNSWIndexUtils::getLowerGraphTableName(tableFuncSharedState->name));
    auto lowerRelTable =
        storageManager->getTable(lowerRelTableEntry->getTableID())->ptrCast<storage::RelTable>();
    // Initialize partitioner shared state.
    const auto partitionerSharedState = tableFuncSharedState->partitionerSharedState;
    partitionerSharedState->setTables(nodeTable, upperRelTable);
    logical_type_vec_t callColumnTypes;
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    callColumnTypes.push_back(LogicalType::INTERNAL_ID());
    tableFuncSharedState->partitionerSharedState->initialize(callColumnTypes, clientContext);
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
    auto dummyCallSink = std::make_unique<processor::DummySink>(
        std::make_unique<processor::ResultSetDescriptor>(logicalOp->getSchema()), std::move(callOp),
        planMapper->getOperatorID(), std::make_unique<OPPrintInfo>());
    processor::physical_op_vector_t children;
    children.push_back(std::move(copyRelUpper));
    children.push_back(std::move(copyRelLower));
    children.push_back(std::move(dummyCallSink));
    return planMapper->createFTableScanAligned({}, logicalOp->getSchema(), fTable,
        DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(children));
}

function_set InternalCreateHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->tableFunc = tableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = initCreateHNSWSharedState;
    func->initLocalStateFunc = initCreateHNSWLocalState;
    func->progressFunc = progressFunc;
    func->finalizeFunc = finalizeFunc;
    func->getPhysicalPlanFunc = getPhysicalPlan;
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set CreateHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->tableFunc = TableFunction::emptyTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = TableFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->rewriteFunc = createHNSWIndexTables;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
