#pragma once

#include "binder/expression/node_expression.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"
#include "storage/index/hnsw_index.h"

namespace kuzu {
namespace function {

struct CreateHNSWIndexBindData final : TableFuncBindData {
    main::ClientContext* context;
    std::string indexName;
    catalog::TableCatalogEntry* tableEntry;
    common::property_id_t propertyID;
    storage::HNSWIndexConfig config;

    CreateHNSWIndexBindData(main::ClientContext* context, std::string indexName,
        catalog::TableCatalogEntry* tableEntry, common::property_id_t propertyID,
        common::offset_t numNodes, storage::HNSWIndexConfig config)
        : TableFuncBindData{numNodes}, context{context}, indexName{std::move(indexName)},
          tableEntry{tableEntry}, propertyID{propertyID}, config{std::move(config)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, propertyID,
            numRows, config.copy());
    }
};

struct CreateInMemHNSWSharedState final : SimpleTableFuncSharedState {
    std::string name;
    std::shared_ptr<storage::InMemHNSWIndex> hnswIndex;
    storage::NodeTable& nodeTable;
    common::offset_t numNodes;
    std::atomic<common::offset_t> numNodesInserted = 0;

    const CreateHNSWIndexBindData* bindData;

    explicit CreateInMemHNSWSharedState(const CreateHNSWIndexBindData& bindData);
};

struct CreateInMemHNSWLocalState final : TableFuncLocalState {
    storage::VisitedState upperVisited;
    storage::VisitedState lowerVisited;

    explicit CreateInMemHNSWLocalState(common::offset_t numNodes)
        : upperVisited{numNodes}, lowerVisited{numNodes} {}
};

struct FinalizeHNSWSharedState final : public SimpleTableFuncSharedState {
    std::shared_ptr<storage::InMemHNSWIndex> hnswIndex;
    std::shared_ptr<storage::HNSWIndexPartitionerSharedState> partitionerSharedState;
    std::unique_ptr<TableFuncBindData> bindData;

    std::atomic<common::node_group_idx_t> numNodeGroupsFinalized = 0;

    explicit FinalizeHNSWSharedState(storage::MemoryManager& mm) : SimpleTableFuncSharedState{} {
        partitionerSharedState = std::make_shared<storage::HNSWIndexPartitionerSharedState>(mm);
    }
};

struct BoundQueryHNSWIndexInput {
    catalog::NodeTableCatalogEntry* nodeTableEntry;
    const graph::GraphEntry* graphEntry;
    catalog::IndexCatalogEntry* indexEntry;
    std::shared_ptr<binder::Expression> queryExpression;
    uint64_t k;
};

struct QueryHNSWIndexBindData final : TableFuncBindData {
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

struct QueryHNSWIndexSharedState final : TableFuncSharedState {
    storage::NodeTable* nodeTable;
    common::offset_t numNodes;

    explicit QueryHNSWIndexSharedState(const QueryHNSWIndexBindData& bindData);
};

struct QueryHNSWLocalState final : TableFuncLocalState {
    std::optional<std::vector<storage::NodeWithDistance>> result;
    storage::HNSWSearchState searchState;
    uint64_t numRowsOutput;

    explicit QueryHNSWLocalState(storage::HNSWSearchState searchState)
        : searchState{std::move(searchState)}, numRowsOutput{0} {}

    bool hasResultToOutput() const { return result.has_value(); }
};

struct InternalCreateHNSWIndexFunction final {
    static constexpr const char* name = "_CREATE_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct InternalFinalizeHNSWIndexFunction final {
    static constexpr const char* name = "_FINALIZE_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct CreateHNSWIndexFunction final {
    static constexpr const char* name = "CREATE_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct InternalDropHNSWIndexFunction final {
    static constexpr const char* name = "_DROP_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct DropHNSWIndexFunction final {
    static constexpr const char* name = "DROP_HNSW_INDEX";

    static function_set getFunctionSet();
};

struct QueryHNSWIndexFunction final {
    static constexpr const char* name = "QUERY_HNSW_INDEX";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
