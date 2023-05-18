#pragma once

#include "common/string_utils.h"
#include "storage/copier/node_copier.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/store/node_table.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class NodeCopyExecutor : public TableCopyExecutor {

public:
    NodeCopyExecutor(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, storage::NodeTable* table,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
        : TableCopyExecutor{copyDescription, std::move(outputDirectory), taskScheduler, catalog,
              table->getTableID(), nodesStatisticsAndDeletedIDs},
          table{table} {}

protected:
    void initializeColumnsAndLists() override {
        // DO NOTHING
    }

    void populateColumnsAndLists(processor::ExecutionContext* executionContext) override;

    void saveToFile() override {
        // DO NOTHING
    }

private:
    void populateColumns(processor::ExecutionContext* executionContext);

private:
    storage::NodeTable* table;
};

} // namespace storage
} // namespace kuzu
