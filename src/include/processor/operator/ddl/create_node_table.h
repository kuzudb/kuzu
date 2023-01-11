#pragma once

#include "processor/operator/ddl/create_table.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace processor {

class CreateNodeTable : public CreateTable {
public:
    CreateNodeTable(Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, uint32_t primaryKeyIdx,
        const DataPos& outputPos, uint32_t id, const string& paramsString,
        NodesStatisticsAndDeletedIDs* nodesStatistics)
        : CreateTable{PhysicalOperatorType::CREATE_NODE_TABLE, catalog, std::move(tableName),
              std::move(propertyNameDataTypes), outputPos, id, paramsString},
          primaryKeyIdx{primaryKeyIdx}, nodesStatistics{nodesStatistics} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNodeTable>(catalog, tableName, propertyNameDataTypes,
            primaryKeyIdx, outputPos, id, paramsString, nodesStatistics);
    }

private:
    uint32_t primaryKeyIdx;
    NodesStatisticsAndDeletedIDs* nodesStatistics;
};

} // namespace processor
} // namespace kuzu
