#pragma once

#include "binder/expression/node_expression.h"
#include "function/table/simple_table_functions.h"
#include "storage/index/hnsw_index.h"

namespace kuzu {
namespace function {

struct CreateHNSWIndexBindData final : SimpleTableFuncBindData {
    main::ClientContext* context;
    std::string indexName;
    catalog::TableCatalogEntry* tableEntry;
    common::column_id_t columnID;
    storage::HNSWIndexConfig config;
    common::offset_t numNodes;

    CreateHNSWIndexBindData(main::ClientContext* context, std::string indexName,
        catalog::TableCatalogEntry* tableEntry, common::column_id_t columnID,
        common::offset_t numNodes, common::offset_t maxOffset, storage::HNSWIndexConfig config)
        : SimpleTableFuncBindData{maxOffset}, context{context}, indexName{std::move(indexName)},
          tableEntry{tableEntry}, columnID{columnID}, config{std::move(config)},
          numNodes{numNodes} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, columnID,
            numNodes, maxOffset, config.copy());
    }
};

struct CreateHNSWSharedState final : SimpleTableFuncSharedState {
    std::string name;
    std::unique_ptr<storage::InMemHNSWIndex> hnswIndex;
    storage::NodeTable& nodeTable;
    common::offset_t numNodes;
    std::atomic<common::offset_t> numNodesInserted = 0;

    const CreateHNSWIndexBindData* bindData;
    std::shared_ptr<storage::HNSWIndexPartitionerSharedState> partitionerSharedState;

    explicit CreateHNSWSharedState(const CreateHNSWIndexBindData& bindData);
};

struct CreateHNSWLocalState final : TableFuncLocalState {
    storage::VisitedState upperVisited;
    storage::VisitedState lowerVisited;

    explicit CreateHNSWLocalState(common::offset_t numNodes)
        : upperVisited{numNodes}, lowerVisited{numNodes} {}
};

struct BoundQueryHNSWIndexInput {
    catalog::NodeTableCatalogEntry* nodeTableEntry;
    catalog::IndexCatalogEntry* indexEntry;
    std::shared_ptr<binder::Expression> queryExpression;
    uint64_t k;
};

struct QueryHNSWIndexBindData final : SimpleTableFuncBindData {
    main::ClientContext* context;
    BoundQueryHNSWIndexInput boundInput;
    common::column_id_t indexColumnID;
    catalog::RelTableCatalogEntry* upperHNSWRelTableEntry;
    catalog::RelTableCatalogEntry* lowerHNSWRelTableEntry;
    storage::QueryHNSWConfig config;

    std::shared_ptr<binder::NodeExpression> outputNode;

    QueryHNSWIndexBindData(main::ClientContext* context, binder::expression_vector columns,
        BoundQueryHNSWIndexInput boundInput, storage::QueryHNSWConfig config,
        std::shared_ptr<binder::NodeExpression> outputNode);

    std::shared_ptr<binder::Expression> getNodeOutput() const override { return outputNode; }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryHNSWIndexBindData>(context, columns, boundInput, config,
            outputNode);
    }
};

struct QueryHNSWIndexSharedState final : SimpleTableFuncSharedState {
    storage::NodeTable* nodeTable;
    common::offset_t numNodes;

    explicit QueryHNSWIndexSharedState(const QueryHNSWIndexBindData& bindData);
};

struct QueryHNSWLocalState final : TableFuncLocalState {
    storage::VisitedState visited;
    std::optional<std::vector<storage::NodeWithDistance>> result;
    uint64_t numRowsOutput = 0;
    storage::OnDiskEmbeddingScanState embeddingScanState;

    QueryHNSWLocalState(storage::MemoryManager* mm, storage::NodeTable& nodeTable,
        common::column_id_t columnID, common::offset_t numNodes)
        : visited{numNodes}, embeddingScanState{mm, nodeTable, columnID} {}

    bool hasResultToOutput() const { return result.has_value(); }
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
