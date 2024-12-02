#pragma once

#include "function/table/call_functions.h"
#include "storage/index/hnsw_index.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace function {

struct CreateHNSWIndexBindData final : StandaloneTableFuncBindData {
    main::ClientContext* context;

    std::string indexName;
    catalog::TableCatalogEntry* tableEntry;
    storage::NodeTable* table;
    common::column_id_t columnID;

    CreateHNSWIndexBindData(main::ClientContext* context, std::string indexName,
        catalog::TableCatalogEntry* tableEntry, storage::NodeTable* table,
        common::column_id_t columnID, common::offset_t numNodes)
        : context{context}, indexName{std::move(indexName)}, tableEntry{tableEntry}, table{table},
          columnID{columnID} {
        this->maxOffset = numNodes > 0 ? numNodes - 1 : 0;
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateHNSWIndexBindData>(context, indexName, tableEntry, table,
            columnID, maxOffset);
    }
};

struct CreateHNSWSharedState final : CallFuncSharedState {
    std::string name;
    std::unique_ptr<storage::InMemHNSWIndex> hnswIndex;
    std::shared_ptr<storage::HNSWIndexPartitionerSharedState> partitionerSharedState;

    explicit CreateHNSWSharedState(const CreateHNSWIndexBindData& bindData)
        : CallFuncSharedState{bindData.maxOffset}, name{bindData.indexName} {
        hnswIndex = std::make_unique<storage::InMemHNSWIndex>(bindData.context, *bindData.table,
            bindData.columnID);
        partitionerSharedState = std::make_shared<storage::HNSWIndexPartitionerSharedState>(
            *bindData.context->getMemoryManager());
    }
};

} // namespace function
} // namespace kuzu
