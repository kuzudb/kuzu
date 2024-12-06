#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "common/types/value/nested.h"
#include "function/table/hnsw/hnsw_index_functions.h"
#include "processor/execution_context.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace function {

// NOLINTNEXTLINE(readability-non-const-parameter)
static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput* input) {
    const auto indexName = input->inputs[0].getValue<std::string>();
    const auto tableName = input->inputs[1].getValue<std::string>();
    const auto& queryVal = input->inputs[2];
    const auto k = input->inputs[3].getValue<int64_t>();

    auto catalog = context->getCatalog();
    const auto nodeTableEntry =
        catalog->getTableCatalogEntry(context->getTx(), tableName);
    const auto indexEntry = common::ku_dynamic_cast<catalog::HNSWIndexCatalogEntry*>(
        catalog->getIndex(context->getTx(), nodeTableEntry->getTableID(), indexName));
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

    auto storageManager = context->getStorageManager();
    auto& nodeTable = storageManager->getTable(nodeTableEntry->getTableID())
                          ->cast<storage::NodeTable>();
    auto& upperRelTable = storageManager->getTable(indexEntry->getUpperRelTableID())
                              ->cast<storage::RelTable>();
    auto& lowerRelTable = storageManager->getTable(indexEntry->getLowerRelTableID())
                              ->cast<storage::RelTable>();
    std::vector<common::LogicalType> columnTypes;
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    columnTypes.push_back(common::LogicalType::FLOAT());
    std::vector<std::string> columnNames = {input->nodeExpression->getInternalID()->toString(), "_distance"};
    auto config = storage::QueryHNSWConfig{input->optionalParams};
    return std::make_unique<QueryHNSWIndexBindData>(std::move(columnTypes), std::move(columnNames),
        nodeTableEntry->getTableID(), indexEntry, std::move(queryVector), k, nodeTable,
        nodeTableEntry->getColumnID(indexEntry->getIndexColumnName()), upperRelTable, lowerRelTable,
        std::move(config), input->nodeExpression);
}

struct QueryHNSWLocalState final : TableFuncLocalState {
    std::optional<std::vector<storage::NodeWithDistance>> result;
    uint64_t numRowsOutput = 0;
};

// NOLINTNEXTLINE(readability-non-const-parameter)
static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    const auto localState = input.localState->ptrCast<QueryHNSWLocalState>();
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    if (!localState->result.has_value()) {
        const auto index = std::make_unique<storage::OnDiskHNSWIndex>(input.context->clientContext,
            bindData->nodeTable, bindData->columnID, bindData->upperRelTable,
            bindData->lowerRelTable, bindData->indexEntry->getConfig());
        index->setUpperEntryPoint(bindData->indexEntry->getUpperEntryPoint());
        index->setLowerEntryPoint(bindData->indexEntry->getLowerEntryPoint());
        localState->result = index->search(bindData->queryVector, bindData->k, bindData->config,
            input.context->clientContext->getTx());
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
            common::internalID_t{nodeOffset, bindData->indexTableID});
        output.dataChunk.getValueVectorMutable(1).setValue<float>(i, distance);
    }
    localState->numRowsOutput += numToOutput;
    output.dataChunk.state->getSelVectorUnsafe().setToUnfiltered(numToOutput);
    return numToOutput;
}

std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput&, TableFuncSharedState*,
    storage::MemoryManager*) {
    return std::make_unique<QueryHNSWLocalState>();
}

function_set QueryHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes{common::LogicalTypeID::STRING, common::LogicalTypeID::STRING,
        common::LogicalTypeID::ARRAY, common::LogicalTypeID::INT64};
    auto tableFunction = std::make_unique<TableFunction>("query_hnsw_index", tableFunc, bindFunc,
        initSharedState, initLocalState, inputTypes);
    tableFunction->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(tableFunction));
    return functionSet;
}

} // namespace function
} // namespace kuzu
