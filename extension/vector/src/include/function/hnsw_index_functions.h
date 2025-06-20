#pragma once

#include "binder/bound_statement.h"
#include "binder/expression/node_expression.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"
#include "function/table/table_function.h"
#include "index/hnsw_index.h"
#include "index/hnsw_rel_batch_insert.h"

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
    std::unique_ptr<GetEmbeddingsScanState> embeddingsScanState;

    explicit CreateInMemHNSWLocalState(common::offset_t numNodes,
        common::offset_t numUpperLayerNodes,
        std::unique_ptr<GetEmbeddingsScanState> embeddingsScanState)
        : upperVisited{numUpperLayerNodes}, lowerVisited{numNodes},
          embeddingsScanState(std::move(embeddingsScanState)) {}
};

struct FinalizeHNSWSharedState final : function::SimpleTableFuncSharedState {
    std::shared_ptr<InMemHNSWIndex> hnswIndex;
    std::shared_ptr<HNSWIndexPartitionerSharedState> partitionerSharedState;
    std::unique_ptr<function::TableFuncBindData> bindData;

    std::atomic<common::node_group_idx_t> numNodeGroupsFinalized = 0;

    explicit FinalizeHNSWSharedState() : SimpleTableFuncSharedState{} {
        partitionerSharedState = std::make_shared<HNSWIndexPartitionerSharedState>();
    }
};

struct QueryHNSWIndexBindData final : function::TableFuncBindData {
    catalog::NodeTableCatalogEntry* nodeTableEntry = nullptr;
    catalog::IndexCatalogEntry* indexEntry = nullptr;
    common::column_id_t indexColumnID = common::INVALID_COLUMN_ID;
    std::shared_ptr<binder::Expression> queryExpression;
    std::shared_ptr<binder::Expression> kExpression;
    std::shared_ptr<binder::NodeExpression> outputNode;
    QueryHNSWConfig config;

    std::shared_ptr<binder::BoundStatement> filterStatement;

    explicit QueryHNSWIndexBindData(binder::expression_vector columns)
        : TableFuncBindData({std::move(columns), 1 /* maxOffset */}) {}

    std::unique_ptr<TableFuncBindData> copy() const override;
};

struct QueryHNSWIndexSharedState final : function::TableFuncSharedState {
    storage::NodeTable* nodeTable;
    common::offset_t numNodes;

    QueryHNSWIndexSharedState(storage::NodeTable* nodeTable, common::offset_t numNodes)
        : TableFuncSharedState{1 /* maxOffset */}, nodeTable{nodeTable}, numNodes{numNodes} {}
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

    static constexpr const char* nnColumnName = "node";
    static constexpr const char* distanceColumnName = "distance";

    static function::function_set getFunctionSet();
};

} // namespace vector_extension
} // namespace kuzu
