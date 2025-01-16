#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
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
    auto& indexAuxInfo =
        this->boundInput.indexEntry->getAuxInfo().cast<catalog::HNSWIndexAuxInfo>();
    indexColumnID = this->boundInput.nodeTableEntry->getColumnID(indexAuxInfo.columnName);
    auto catalog = context->getCatalog();
    upperHNSWRelTableEntry =
        catalog->getTableCatalogEntry(context->getTransaction(), indexAuxInfo.upperRelTableID)
            ->ptrCast<catalog::RelTableCatalogEntry>();
    lowerHNSWRelTableEntry =
        catalog->getTableCatalogEntry(context->getTransaction(), indexAuxInfo.lowerRelTableID)
            ->ptrCast<catalog::RelTableCatalogEntry>();
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    context->setToUseInternalCatalogEntry();
    const auto indexName = input->getLiteralVal<std::string>(0);
    const auto tableName = input->getLiteralVal<std::string>(1);
    auto inputQueryExpression = input->params[2];
    const auto k = input->getLiteralVal<int64_t>(3);

    auto nodeTableEntry = storage::IndexUtils::bindTable(*context, tableName, indexName,
        storage::IndexOperation::QUERY);
    auto indexEntry = context->getCatalog()->getIndex(context->getTransaction(),
        nodeTableEntry->getTableID(), indexName);
    const auto& auxInfo = indexEntry->getAuxInfo().cast<catalog::HNSWIndexAuxInfo>();
    KU_ASSERT(nodeTableEntry->getProperty(auxInfo.columnName).getType().getLogicalTypeID() ==
              common::LogicalTypeID::ARRAY);
    KU_UNUSED(auxInfo);

    auto outputNode = input->binder->createQueryNode("nn", {nodeTableEntry});
    input->binder->addToScope(outputNode->toString(), outputNode);
    binder::expression_vector columns;
    columns.push_back(outputNode->getInternalID());
    columns.push_back(input->binder->createVariable("_distance", common::LogicalType::DOUBLE()));
    auto boundInput = BoundQueryHNSWIndexInput{nodeTableEntry, indexEntry,
        std::move(inputQueryExpression), static_cast<uint64_t>(k)};
    auto config = storage::QueryHNSWConfig{input->optionalParams};
    return std::make_unique<QueryHNSWIndexBindData>(context, std::move(columns), boundInput, config,
        outputNode);
}

static common::PhysicalTypeID getChildQueryValType(const common::Value& value) {
    auto childValType = common::PhysicalTypeID::ANY;
    switch (value.getDataType().getPhysicalType()) {
    case common::PhysicalTypeID::ARRAY: {
        childValType = value.getDataType()
                           .getExtraTypeInfo()
                           ->constPtrCast<common::ArrayTypeInfo>()
                           ->getChildType()
                           .getPhysicalType();
    } break;
    case common::PhysicalTypeID::LIST: {
        childValType = value.getDataType()
                           .getExtraTypeInfo()
                           ->constPtrCast<common::ListTypeInfo>()
                           ->getChildType()
                           .getPhysicalType();
    } break;
    default: {
        throw common::RuntimeException(
            common::stringFormat("Unsupported data type {} as a query vector in {}",
                value.getDataType().toString(), QueryHNSWIndexFunction::name));
    }
    }
    return childValType;
}

template<typename T>
static void convertQueryVector(const common::Value& value, std::vector<float>& queryVector) {
    auto numElements = value.getChildrenSize();
    queryVector.resize(numElements);
    for (auto i = 0u; i < queryVector.size(); i++) {
        queryVector[i] = common::NestedVal::getChildVal(&value, i)->getValue<T>();
    }
}

static common::Value getQueryValue(const binder::Expression& queryExpression) {
    auto value = common::Value::createDefaultValue(queryExpression.dataType);
    switch (queryExpression.expressionType) {
    case common::ExpressionType::LITERAL: {
        value = queryExpression.constCast<binder::LiteralExpression>().getValue();
    } break;
    case common::ExpressionType::PARAMETER: {
        value = queryExpression.constCast<binder::ParameterExpression>().getValue();
    } break;
    default: {
        throw common::RuntimeException(common::stringFormat("Unsupported expression type {} in {}",
            common::ExpressionTypeUtil::toString(queryExpression.expressionType),
            QueryHNSWIndexFunction::name));
    }
    }
    return value;
}

static std::vector<float> getQueryVector(const binder::Expression& queryExpression,
    uint64_t dimension) {
    auto value = getQueryValue(queryExpression);
    common::PhysicalTypeID childValType = getChildQueryValType(value);
    std::vector<float> queryVector;
    if (value.getChildrenSize() != dimension) {
        throw common::RuntimeException("Query vector dimension does not match index dimension.");
    }
    switch (childValType) {
    case common::PhysicalTypeID::FLOAT: {
        convertQueryVector<float>(value, queryVector);
    } break;
    case common::PhysicalTypeID::DOUBLE: {
        convertQueryVector<double>(value, queryVector);
    } break;
    default: {
        throw common::RuntimeException(
            common::stringFormat("Unsupported data type {} as a query vector in {}",
                value.getDataType().toString(), QueryHNSWIndexFunction::name));
    }
    }
    return queryVector;
}

static const common::LogicalType& getIndexColumnType(const BoundQueryHNSWIndexInput& boundInput) {
    auto columnName =
        boundInput.indexEntry->getAuxInfo().cast<catalog::HNSWIndexAuxInfo>().columnName;
    return boundInput.nodeTableEntry->getProperty(columnName).getType();
}

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    const auto localState = input.localState->ptrCast<QueryHNSWLocalState>();
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    // As `k` can be larger than the default vector capacity, we run the actual search in the first
    // call, and output the rest of the query result in chunks in following calls.
    if (!localState->hasResultToOutput()) {
        // We start searching when there is no query result to output.
        const auto& auxInfo =
            bindData->boundInput.indexEntry->getAuxInfo().cast<catalog::HNSWIndexAuxInfo>();
        const auto index = std::make_unique<storage::OnDiskHNSWIndex>(input.context->clientContext,
            bindData->boundInput.nodeTableEntry, bindData->indexColumnID,
            bindData->upperHNSWRelTableEntry, bindData->lowerHNSWRelTableEntry,
            auxInfo.config.copy());
        index->setDefaultUpperEntryPoint(auxInfo.upperEntryPoint);
        index->setDefaultLowerEntryPoint(auxInfo.lowerEntryPoint);

        auto dimension =
            common::ArrayType::getNumElements(getIndexColumnType(bindData->boundInput));
        auto queryVector = getQueryVector(*bindData->boundInput.queryExpression, dimension);
        localState->result = index->search(input.context->clientContext->getTransaction(),
            queryVector, bindData->boundInput.k, bindData->config, localState->visited,
            *localState->embeddingScanState.scanState);
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
