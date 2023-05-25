#pragma once

#include "storage/copier/node_copier.h"
#include "storage/copier/table_copy_utils.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/store/node_table.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class NodeCopyExecutor {

public:
    NodeCopyExecutor(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, storage::NodeTable* table,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
        : copyDescription{copyDescription}, outputDirectory{std::move(outputDirectory)},
          taskScheduler{taskScheduler}, catalog{catalog},
          tableSchema{catalog.getReadOnlyVersion()->getTableSchema(table->getTableID())},
          table{table}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, numRows{0} {}

public:
    common::offset_t copy(processor::ExecutionContext* executionContext);

private:
    void populateColumns(processor::ExecutionContext* executionContext);

private:
    common::CopyDescription& copyDescription;
    std::string outputDirectory;
    common::TaskScheduler& taskScheduler;
    catalog::Catalog& catalog;
    catalog::TableSchema* tableSchema;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    storage::NodeTable* table;
    std::unordered_map<std::string, FileBlockInfo> fileBlockInfos;
    uint64_t numRows;
};

} // namespace storage
} // namespace kuzu
