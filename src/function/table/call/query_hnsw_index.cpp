#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "common/types/value/nested.h"
#include "function/table/call_functions.h"
#include "processor/execution_context.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace function {

struct QueryHNSWIndexBindData final : CallTableFuncBindData {
    common::table_id_t indexTableID;
    catalog::HNSWIndexCatalogEntry* indexEntry;
    std::vector<float> queryVector;
    uint64_t k;
    storage::NodeTable& nodeTable;
    common::column_id_t columnID;
    storage::RelTable& upperRelTable;
    storage::RelTable& lowerRelTable;

    QueryHNSWIndexBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, common::table_id_t indexTableID,
        catalog::HNSWIndexCatalogEntry* indexEntry, std::vector<float> queryVector, uint64_t k,
        storage::NodeTable& nodeTable, common::column_id_t columnID,
        storage::RelTable& upperRelTable, storage::RelTable& lowerRelTable)
        : CallTableFuncBindData{std::move(columnTypes), std::move(columnNames), 1 /*maxOffset*/},
          indexTableID{indexTableID}, indexEntry{indexEntry}, queryVector{std::move(queryVector)},
          k{k}, nodeTable{nodeTable}, columnID{columnID}, upperRelTable{upperRelTable},
          lowerRelTable{lowerRelTable} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryHNSWIndexBindData>(common::LogicalType::copy(columnTypes),
            columnNames, indexTableID, indexEntry, queryVector, k, nodeTable, columnID,
            upperRelTable, lowerRelTable);
    }
};

// NOLINTNEXTLINE(readability-non-const-parameter)
static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput* input) {
    const auto indexName = input->inputs[0].getValue<std::string>();
    const auto tableName = input->inputs[1].getValue<std::string>();
    const auto& queryVal = input->inputs[2];
    std::vector<float> queryVector;
    KU_ASSERT(queryVal.getChildrenSize() > 0);
    queryVector.resize(queryVal.getChildrenSize());
    for (auto i = 0u; i < queryVector.size(); i++) {
        queryVector[i] = common::NestedVal::getChildVal(&queryVal, i)->getValue<float>();
    }
    const auto k = input->inputs[3].getValue<int64_t>();

    const auto nodeTableEntry =
        context->getCatalog()->getTableCatalogEntry(context->getTx(), tableName);
    const auto indexEntry = common::ku_dynamic_cast<catalog::HNSWIndexCatalogEntry*>(
        context->getCatalog()->getIndex(context->getTx(), nodeTableEntry->getTableID(), indexName));
    auto& indexColumnType = nodeTableEntry->getProperty(indexEntry->getIndexColumnName()).getType();
    KU_ASSERT(indexColumnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto dimension =
        indexColumnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>()->getNumElements();
    storage::HNSWIndexUtils::validateQueryVector(queryVal.getDataType(), dimension);

    auto& nodeTable = context->getStorageManager()
                          ->getTable(nodeTableEntry->getTableID())
                          ->cast<storage::NodeTable>();
    auto& upperRelTable = context->getStorageManager()
                              ->getTable(indexEntry->getUpperRelTableID())
                              ->cast<storage::RelTable>();
    auto& lowerRelTable = context->getStorageManager()
                              ->getTable(indexEntry->getLowerRelTableID())
                              ->cast<storage::RelTable>();

    std::vector<common::LogicalType> columnTypes;
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    columnTypes.push_back(common::LogicalType::FLOAT());
    std::vector<std::string> columnNames = {"nn", "_distance"};
    auto boundData = std::make_unique<QueryHNSWIndexBindData>(std::move(columnTypes),
        std::move(columnNames), nodeTableEntry->getTableID(), indexEntry, std::move(queryVector), k,
        nodeTable, nodeTableEntry->getColumnID(indexEntry->getIndexColumnName()), upperRelTable,
        lowerRelTable);
    boundData->columnID = nodeTableEntry->getColumnID(indexEntry->getIndexColumnName());
    return boundData;
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
            bindData->lowerRelTable);
        index->setUpperEntryPoint(bindData->indexEntry->getUpperEntryPoint());
        index->setLowerEntryPoint(bindData->indexEntry->getLowerEntryPoint());
        localState->result = index->search(bindData->queryVector, bindData->k,
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
