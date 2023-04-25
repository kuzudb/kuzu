#pragma once

#include "processor/operator/ddl/create_table.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace processor {

class CreateNodeTable : public CreateTable {
public:
    CreateNodeTable(catalog::Catalog* catalog, std::string tableName,
        std::vector<catalog::Property> properties, uint32_t primaryKeyIdx, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString,
        storage::NodesStatisticsAndDeletedIDs* nodesStatistics)
        : CreateTable{PhysicalOperatorType::CREATE_NODE_TABLE, catalog, std::move(tableName),
              std::move(properties), outputPos, id, paramsString},
          primaryKeyIdx{primaryKeyIdx}, nodesStatistics{nodesStatistics} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CreateNodeTable>(catalog, tableName, properties, primaryKeyIdx,
            outputPos, id, paramsString, nodesStatistics);
    }

private:
    uint32_t primaryKeyIdx;
    storage::NodesStatisticsAndDeletedIDs* nodesStatistics;
};

} // namespace processor
} // namespace kuzu
