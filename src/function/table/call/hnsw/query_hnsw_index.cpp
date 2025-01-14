#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/value/nested.h"
#include "function/table/hnsw/hnsw_index_functions.h"
#include "processor/execution_context.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/index/index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace function {

QueryHNSWIndexBindData::QueryHNSWIndexBindData(main::ClientContext* context,
    binder::expression_vector columns, BoundQueryHNSWIndexInput boundInput,
    storage::QueryHNSWConfig config, std::shared_ptr<binder::NodeExpression> outputNode)
    : SimpleTableFuncBindData{std::move(columns), 1 /*maxOffset*/}, context{context},
      boundInput{std::move(boundInput)}, config{config}, outputNode{std::move(outputNode)} {
    indexColumnID = this->boundInput.nodeTableEntry->getColumnID(
        this->boundInput.indexEntry->getIndexColumnName());
    upperHNSWRelTableEntry = context->getCatalog()
                                 ->getTableCatalogEntry(context->getTransaction(),
                                     this->boundInput.indexEntry->getUpperRelTableID())
                                 ->ptrCast<catalog::RelTableCatalogEntry>();
    lowerHNSWRelTableEntry = context->getCatalog()
                                 ->getTableCatalogEntry(context->getTransaction(),
                                     this->boundInput.indexEntry->getLowerRelTableID())
                                 ->ptrCast<catalog::RelTableCatalogEntry>();
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto indexName = input->getLiteralVal<std::string>(0);
    const auto tableName = input->getLiteralVal<std::string>(1);
    const auto& queryVal = input->params[2]->constCast<binder::LiteralExpression>().getValue();
    const auto k = input->getLiteralVal<int64_t>(3);

    auto nodeTableEntry = storage::IndexUtils::bindTable(*context, tableName, indexName,
        storage::IndexOperation::QUERY);
    const auto indexEntry = common::ku_dynamic_cast<catalog::HNSWIndexCatalogEntry*>(
        context->getCatalog()->getIndex(context->getTransaction(), nodeTableEntry->getTableID(),
            indexName));
    auto& indexColumnType = nodeTableEntry->getProperty(indexEntry->getIndexColumnName()).getType();
    KU_ASSERT(indexColumnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto dimension =
        indexColumnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>()->getNumElements();
    storage::HNSWIndexUtils::validateQueryVector(queryVal.getDataType(), dimension);

    std::vector<float> queryVector;
    KU_ASSERT(queryVal.getChildrenSize() > 0);
    queryVector.resize(queryVal.getChildrenSize());
    for (auto i = 0u; i < queryVector.size(); i++) {
        queryVector[i] = common::NestedVal::getChildVal(&queryVal, i)->getValue<float>();
    }

    auto outputNode = input->binder->createQueryNode("nn", {nodeTableEntry});
    input->binder->addToScope(outputNode->toString(), outputNode);
    binder::expression_vector columns;
    columns.push_back(outputNode->getInternalID());
    columns.push_back(input->binder->createVariable("_distance", common::LogicalType::DOUBLE()));
    auto boundInput = BoundQueryHNSWIndexInput{nodeTableEntry, indexEntry, std::move(queryVector),
        static_cast<uint64_t>(k)};
    auto config = storage::QueryHNSWConfig{input->optionalParams};
    return std::make_unique<QueryHNSWIndexBindData>(context, std::move(columns), boundInput, config,
        outputNode);
}

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    const auto localState = input.localState->ptrCast<QueryHNSWLocalState>();
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    // As `k` can be larger than the default vector capacity, we run the actual search in the first
    // call, and output the rest of the query result in chunks in following calls.
    if (!localState->hasResultToOutput()) {
        // We start searching when there is no query result to output.
        const auto index = std::make_unique<storage::OnDiskHNSWIndex>(input.context->clientContext,
            bindData->boundInput.nodeTableEntry, bindData->indexColumnID,
            bindData->upperHNSWRelTableEntry, bindData->lowerHNSWRelTableEntry,
            bindData->boundInput.indexEntry->getConfig().copy());
        index->setDefaultUpperEntryPoint(bindData->boundInput.indexEntry->getUpperEntryPoint());
        index->setDefaultLowerEntryPoint(bindData->boundInput.indexEntry->getLowerEntryPoint());
        localState->result = index->search(input.context->clientContext->getTransaction(),
            bindData->boundInput.queryVector, bindData->boundInput.k, bindData->config,
            localState->visited, *localState->embeddingScanState.scanState);
    }
    KU_ASSERT(localState->result.has_value());
    if (localState->numRowsOutput >= localState->result->size()) {
        return 0;
    }
    const auto numToOutput = std::min(localState->result->size() - localState->numRowsOutput,
        common::DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numToOutput; i++) {
        const auto& [nodeOffset, distance] =
            localState->result.value()[i + localState->numRowsOutput];
        output.dataChunk.getValueVectorMutable(0).setValue<common::internalID_t>(i,
            common::internalID_t{nodeOffset, bindData->boundInput.nodeTableEntry->getTableID()});
        output.dataChunk.getValueVectorMutable(1).setValue<double>(i, distance);
    }
    localState->numRowsOutput += numToOutput;
    output.dataChunk.state->getSelVectorUnsafe().setToUnfiltered(numToOutput);
    return numToOutput;
}

QueryHNSWIndexSharedState::QueryHNSWIndexSharedState(const QueryHNSWIndexBindData& bindData)
    : SimpleTableFuncSharedState{1 /* maxOffset */} {
    const auto storageManager = bindData.context->getStorageManager();
    nodeTable = storageManager->getTable(bindData.boundInput.nodeTableEntry->getTableID())
                    ->ptrCast<storage::NodeTable>();
    numNodes = nodeTable->getStats(bindData.context->getTransaction()).getTableCard();
}

static std::unique_ptr<TableFuncSharedState> initQueryHNSWSharedState(
    const TableFunctionInitInput& input) {
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    return std::make_unique<QueryHNSWIndexSharedState>(*bindData);
}

std::unique_ptr<TableFuncLocalState> initQueryHNSWLocalState(const TableFunctionInitInput& input,
    TableFuncSharedState* sharedState, storage::MemoryManager* mm) {
    const auto hnswBindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    const auto hnswSharedState = sharedState->ptrCast<QueryHNSWIndexSharedState>();
    return std::make_unique<QueryHNSWLocalState>(mm, *hnswSharedState->nodeTable,
        hnswBindData->indexColumnID, hnswSharedState->numNodes);
}

function_set QueryHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes{common::LogicalTypeID::STRING, common::LogicalTypeID::STRING,
        common::LogicalTypeID::ARRAY, common::LogicalTypeID::INT64};
    auto tableFunction = std::make_unique<TableFunction>("query_hnsw_index", inputTypes);
    tableFunction->tableFunc = tableFunc;
    tableFunction->bindFunc = bindFunc;
    tableFunction->initSharedStateFunc = initQueryHNSWSharedState;
    tableFunction->initLocalStateFunc = initQueryHNSWLocalState;
    tableFunction->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(tableFunction));
    return functionSet;
}

} // namespace function
} // namespace kuzu
