#pragma once

#include "function/table/call_functions.h"
#include "storage/index/hnsw_index.h"
#include "storage/store/node_table.h"
#include "binder/expression/node_expression.h"
#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"

namespace kuzu {
namespace function {

struct CreateHNSWIndexBindData final : CallTableFuncBindData {
    main::ClientContext* context;

    std::string indexName;
    catalog::TableCatalogEntry* tableEntry;
    storage::NodeTable* table;
    common::column_id_t columnID;

    storage::HNSWIndexConfig config;

    CreateHNSWIndexBindData(main::ClientContext* context, std::string indexName,
        catalog::TableCatalogEntry* tableEntry, storage::NodeTable* table,
        common::column_id_t columnID, common::offset_t numNodes, storage::HNSWIndexConfig config)
        : CallTableFuncBindData{numNodes > 0 ? numNodes - 1 : 0}, context{context}, indexName{std::move(indexName)}, tableEntry{tableEntry}, table{table},
          columnID{columnID}, config{std::move(config)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, table,
            columnID, maxOffset, config);
    }
};

struct CreateHNSWSharedState final : CallFuncSharedState {
    std::string name;
    std::unique_ptr<storage::InMemHNSWIndex> hnswIndex;
    std::shared_ptr<storage::HNSWIndexPartitionerSharedState> partitionerSharedState;

    explicit CreateHNSWSharedState(const CreateHNSWIndexBindData& bindData)
        : CallFuncSharedState{bindData.maxOffset}, name{bindData.indexName} {
        hnswIndex = std::make_unique<storage::InMemHNSWIndex>(bindData.context, *bindData.table,
            bindData.columnID, bindData.config);
        partitionerSharedState = std::make_shared<storage::HNSWIndexPartitionerSharedState>(
            *bindData.context->getMemoryManager());
    }
};

struct QueryHNSWIndexBindData final : CallTableFuncBindData {
    common::table_id_t indexTableID;
    catalog::HNSWIndexCatalogEntry* indexEntry;
    std::vector<float> queryVector;
    uint64_t k;
    storage::NodeTable& nodeTable;
    common::column_id_t columnID;
    storage::RelTable& upperRelTable;
    storage::RelTable& lowerRelTable;
    storage::QueryHNSWConfig config;

    std::shared_ptr<binder::NodeExpression> outputNode;

    QueryHNSWIndexBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, common::table_id_t indexTableID,
        catalog::HNSWIndexCatalogEntry* indexEntry, std::vector<float> queryVector, uint64_t k,
        storage::NodeTable& nodeTable, common::column_id_t columnID,
        storage::RelTable& upperRelTable, storage::RelTable& lowerRelTable,
        storage::QueryHNSWConfig config, std::shared_ptr<binder::NodeExpression> outputNode)
        : CallTableFuncBindData{std::move(columnTypes), std::move(columnNames), 1 /*maxOffset*/},
          indexTableID{indexTableID}, indexEntry{indexEntry}, queryVector{std::move(queryVector)},
          k{k}, nodeTable{nodeTable}, columnID{columnID}, upperRelTable{upperRelTable},
          lowerRelTable{lowerRelTable}, config{std::move(config)}, outputNode{std::move(outputNode)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryHNSWIndexBindData>(common::LogicalType::copy(columnTypes),
            columnNames, indexTableID, indexEntry, queryVector, k, nodeTable, columnID,
            upperRelTable, lowerRelTable, config, outputNode);
    }
};

struct CreateHNSWIndexFunction : public CallFunction {
    static constexpr const char* name = "CREATE_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct QueryHNSWIndexFunction : public CallFunction {
    static constexpr const char* name = "QUERY_HNSW_INDEX";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
