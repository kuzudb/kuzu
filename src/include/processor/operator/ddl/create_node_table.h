#pragma once

#include "processor/operator/ddl/create_table.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "storage/store/nodes_store.h"

namespace kuzu {
namespace processor {

class CreateNodeTable : public CreateTable {
public:
    CreateNodeTable(catalog::Catalog* catalog, std::string tableName,
        std::vector<std::unique_ptr<catalog::Property>> properties, uint32_t primaryKeyIdx,
        storage::StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString, storage::NodesStatisticsAndDeletedIDs* nodesStatistics)
        : CreateTable{PhysicalOperatorType::CREATE_NODE_TABLE, catalog, std::move(tableName),
              std::move(properties), outputPos, id, paramsString},
          primaryKeyIdx{primaryKeyIdx}, storageManager{storageManager}, nodesStatistics{
                                                                            nodesStatistics} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CreateNodeTable>(catalog, tableName,
            catalog::Property::copyProperties(properties), primaryKeyIdx, storageManager, outputPos,
            id, paramsString, nodesStatistics);
    }

private:
    uint32_t primaryKeyIdx;
    storage::StorageManager& storageManager;
    storage::NodesStatisticsAndDeletedIDs* nodesStatistics;
};

} // namespace processor
} // namespace kuzu
