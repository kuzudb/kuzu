#pragma once

#include "binder/expression/node_expression.h"
#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "function/table/simple_table_functions.h"
#include "storage/index/hnsw_index.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace function {

struct CreateHNSWIndexBindData final : SimpleTableFuncBindData {
    main::ClientContext* context;

    std::string indexName;
    catalog::TableCatalogEntry* tableEntry;
    storage::NodeTable* table;
    common::column_id_t columnID;

    storage::HNSWIndexConfig config;

    CreateHNSWIndexBindData(main::ClientContext* context, std::string indexName,
        catalog::TableCatalogEntry* tableEntry, storage::NodeTable* table,
        common::column_id_t columnID, common::offset_t numNodes, storage::HNSWIndexConfig config)
        : SimpleTableFuncBindData{numNodes > 0 ? numNodes - 1 : 0}, context{context},
          indexName{std::move(indexName)}, tableEntry{tableEntry}, table{table}, columnID{columnID},
          config{std::move(config)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, table,
            columnID, maxOffset, config);
    }
};

struct CreateHNSWSharedState final : SimpleTableFuncSharedState {
    std::string name;
    std::unique_ptr<storage::InMemHNSWIndex> hnswIndex;
    std::shared_ptr<storage::HNSWIndexPartitionerSharedState> partitionerSharedState;

    common::offset_t numNodes;
    std::atomic<common::offset_t> numNodesInserted = 0;

    const CreateHNSWIndexBindData* bindData = nullptr;

    explicit CreateHNSWSharedState(const CreateHNSWIndexBindData& bindData)
        : SimpleTableFuncSharedState{bindData.maxOffset}, name{bindData.indexName},
          numNodes{bindData.maxOffset + 1} {
        hnswIndex = std::make_unique<storage::InMemHNSWIndex>(bindData.context, *bindData.table,
            bindData.columnID, bindData.config);
        partitionerSharedState = std::make_shared<storage::HNSWIndexPartitionerSharedState>(
            *bindData.context->getMemoryManager());
    }
};

struct CreateHNSWLocalState final : TableFuncLocalState {
    storage::VisitedState visited;

    explicit CreateHNSWLocalState(common::offset_t numNodes) : visited{numNodes} {}
};

struct QueryHNSWIndexBindData final : SimpleTableFuncBindData {
    common::table_id_t indexTableID;
    catalog::HNSWIndexCatalogEntry* indexEntry;
    std::vector<float> queryVector;
    uint64_t k;
    storage::NodeTable& nodeTable;
    common::column_id_t columnID;
    common::offset_t numNodes;
    storage::RelTable& upperRelTable;
    storage::RelTable& lowerRelTable;
    storage::QueryHNSWConfig config;

    std::shared_ptr<binder::NodeExpression> outputNode;

    QueryHNSWIndexBindData(binder::expression_vector columns, common::table_id_t indexTableID,
        catalog::HNSWIndexCatalogEntry* indexEntry, std::vector<float> queryVector, uint64_t k,
        storage::NodeTable& nodeTable, common::column_id_t columnID, common::offset_t numNodes,
        storage::RelTable& upperRelTable, storage::RelTable& lowerRelTable,
        storage::QueryHNSWConfig config, std::shared_ptr<binder::NodeExpression> outputNode)
        : SimpleTableFuncBindData{std::move(columns), 1 /*maxOffset*/}, indexTableID{indexTableID},
          indexEntry{indexEntry}, queryVector{std::move(queryVector)}, k{k}, nodeTable{nodeTable},
          columnID{columnID}, numNodes{numNodes}, upperRelTable{upperRelTable},
          lowerRelTable{lowerRelTable}, config{std::move(config)},
          outputNode{std::move(outputNode)} {}

    std::shared_ptr<binder::Expression> getNodeOutput() const override {
        return outputNode;
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryHNSWIndexBindData>(columns, indexTableID, indexEntry,
            queryVector, k, nodeTable, columnID, numNodes, upperRelTable, lowerRelTable, config,
            outputNode);
    }
};

struct QueryHNSWLocalState final : TableFuncLocalState {
    storage::VisitedState visited;
    std::optional<std::vector<storage::NodeWithDistance>> result;
    uint64_t numRowsOutput = 0;

    explicit QueryHNSWLocalState(common::offset_t numNodes) : visited{numNodes} {}
};

struct CreateHNSWIndexFunction final : SimpleTableFunction {
    static constexpr const char* name = "CREATE_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct DropHNSWIndexFunction final : SimpleTableFunction {
    static constexpr const char* name = "DROP_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct QueryHNSWIndexFunction final : SimpleTableFunction {
    static constexpr const char* name = "QUERY_HNSW_INDEX";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
