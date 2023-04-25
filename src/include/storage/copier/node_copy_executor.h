#pragma once

#include "common/string_utils.h"
#include "storage/copier/node_copier.h"
#include "storage/in_mem_storage_structure/in_mem_node_column.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class NodeCopyExecutor : public TableCopyExecutor {

public:
    NodeCopyExecutor(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
        : TableCopyExecutor{copyDescription, std::move(outputDirectory), taskScheduler, catalog,
              tableID, nodesStatisticsAndDeletedIDs} {}

protected:
    void initializeColumnsAndLists() override;

    void populateColumnsAndLists(processor::ExecutionContext* executionContext) override;

    // TODO(Guodong): do we need this? should go to finalize.
    void saveToFile() override;

    std::unordered_map<common::property_id_t, common::column_id_t> propertyIDToColumnIDMap;
    std::vector<std::unique_ptr<InMemNodeColumn>> columns;

private:
    void populateColumns(processor::ExecutionContext* executionContext);
};

} // namespace storage
} // namespace kuzu
