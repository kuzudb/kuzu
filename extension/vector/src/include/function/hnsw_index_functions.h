#pragma once

#include "binder/expression/node_expression.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"
#include "function/table/table_function.h"
#include "index/hnsw_index.h"

namespace kuzu {
namespace vector_extension {

struct CreateHNSWIndexBindData final : function::TableFuncBindData {
    main::ClientContext* context;
    std::string indexName;
    catalog::TableCatalogEntry* tableEntry;
    common::property_id_t propertyID;
    HNSWIndexConfig config;

    CreateHNSWIndexBindData(main::ClientContext* context, std::string indexName,
        catalog::TableCatalogEntry* tableEntry, common::property_id_t propertyID,
        common::offset_t numNodes, HNSWIndexConfig config)
        : TableFuncBindData{numNodes}, context{context}, indexName{std::move(indexName)},
          tableEntry{tableEntry}, propertyID{propertyID}, config{std::move(config)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, propertyID,
            numRows, config.copy());
    }
};

struct CreateInMemHNSWSharedState final : function::SimpleTableFuncSharedState {
    std::string name;
    std::shared_ptr<InMemHNSWIndex> hnswIndex;
    storage::NodeTable& nodeTable;
    common::offset_t numNodes;
    std::atomic<common::offset_t> numNodesInserted = 0;

    const CreateHNSWIndexBindData* bindData;

    explicit CreateInMemHNSWSharedState(const CreateHNSWIndexBindData& bindData);
};

struct CreateInMemHNSWLocalState final : function::TableFuncLocalState {
    VisitedState upperVisited;
    VisitedState lowerVisited;

    explicit CreateInMemHNSWLocalState(common::offset_t numNodes)
        : upperVisited{numNodes}, lowerVisited{numNodes} {}
};

struct FinalizeHNSWSharedState final : function::SimpleTableFuncSharedState {
    std::shared_ptr<InMemHNSWIndex> hnswIndex;
    std::shared_ptr<HNSWIndexPartitionerSharedState> partitionerSharedState;
    std::unique_ptr<function::TableFuncBindData> bindData;

    std::atomic<common::node_group_idx_t> numNodeGroupsFinalized = 0;

    explicit FinalizeHNSWSharedState(storage::MemoryManager& mm) : SimpleTableFuncSharedState{} {
        partitionerSharedState = std::make_shared<HNSWIndexPartitionerSharedState>(mm);
    }
};

struct BoundQueryHNSWIndexInput {
    catalog::NodeTableCatalogEntry* nodeTableEntry;
    graph::GraphEntry graphEntry;
    catalog::IndexCatalogEntry* indexEntry;
    std::shared_ptr<binder::Expression> queryExpression;
    uint64_t k;

    BoundQueryHNSWIndexInput(catalog::NodeTableCatalogEntry* nodeTableEntry,
        graph::GraphEntry graphEntry, catalog::IndexCatalogEntry* indexEntry,
        std::shared_ptr<binder::Expression> queryExpression, uint64_t k)
        : nodeTableEntry{nodeTableEntry}, graphEntry{std::move(graphEntry)}, indexEntry{indexEntry},
          queryExpression{std::move(queryExpression)}, k{k} {}
    BoundQueryHNSWIndexInput(const BoundQueryHNSWIndexInput& rhs)
        : nodeTableEntry{rhs.nodeTableEntry}, graphEntry{rhs.graphEntry.copy()},
          indexEntry{rhs.indexEntry}, queryExpression{rhs.queryExpression}, k{rhs.k} {}
};

struct QueryHNSWIndexBindData final : function::TableFuncBindData {
    main::ClientContext* context;
    BoundQueryHNSWIndexInput boundInput;
    common::column_id_t indexColumnID;
    catalog::RelTableCatalogEntry* upperHNSWRelTableEntry;
    catalog::RelTableCatalogEntry* lowerHNSWRelTableEntry;
    QueryHNSWConfig config;

    std::shared_ptr<binder::NodeExpression> outputNode;

    QueryHNSWIndexBindData(main::ClientContext* context, binder::expression_vector columns,
        const BoundQueryHNSWIndexInput& boundInput, QueryHNSWConfig config,
        std::shared_ptr<binder::NodeExpression> outputNode);

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryHNSWIndexBindData>(context, columns, boundInput, config,
            outputNode);
    }
};

struct QueryHNSWIndexSharedState final : function::TableFuncSharedState {
    storage::NodeTable* nodeTable;
    common::offset_t numNodes;

    explicit QueryHNSWIndexSharedState(const QueryHNSWIndexBindData& bindData);
};

struct QueryHNSWLocalState final : function::TableFuncLocalState {
    std::optional<std::vector<NodeWithDistance>> result;
    HNSWSearchState searchState;
    uint64_t numRowsOutput;

    explicit QueryHNSWLocalState(HNSWSearchState searchState)
        : searchState{std::move(searchState)}, numRowsOutput{0} {}

    bool hasResultToOutput() const { return result.has_value(); }
};

struct InternalCreateHNSWIndexFunction final {
    static constexpr const char* name = "_CREATE_HNSW_INDEX";

    static function::function_set getFunctionSet();
};

struct InternalFinalizeHNSWIndexFunction final {
    static constexpr const char* name = "_FINALIZE_HNSW_INDEX";

    static function::function_set getFunctionSet();
};

struct InternalDropHNSWIndexFunction final {
    static constexpr const char* name = "_DROP_HNSW_INDEX";

    static function::function_set getFunctionSet();
};

struct CreateVectorIndexFunction final {
    static constexpr const char* name = "CREATE_VECTOR_INDEX";

    static function::function_set getFunctionSet();
};

struct DropVectorIndexFunction final {
    static constexpr const char* name = "DROP_VECTOR_INDEX";

    static function::function_set getFunctionSet();
};

struct QueryVectorIndexFunction final {
    static constexpr const char* name = "QUERY_VECTOR_INDEX";

    static constexpr const char* nnColumnName = "nn";
    static constexpr const char* distanceColumnName = "distance";

    static function::function_set getFunctionSet();
};

} // namespace vector_extension
} // namespace kuzu
