#pragma once

#include "processor/operator/ddl/create_table.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

class CreateRelTable : public CreateTable {
public:
    CreateRelTable(catalog::Catalog* catalog, std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes,
        catalog::RelMultiplicity relMultiplicity,
        std::vector<std::pair<common::table_id_t, common::table_id_t>> srcDstTableIDs,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString,
        storage::RelsStatistics* relsStatistics)
        : CreateTable{PhysicalOperatorType::CREATE_REL_TABLE, catalog, std::move(tableName),
              std::move(propertyNameDataTypes), outputPos, id, paramsString},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{std::move(srcDstTableIDs)},
          relsStatistics{relsStatistics} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTable>(catalog, tableName, propertyNameDataTypes,
            relMultiplicity, srcDstTableIDs, outputPos, id, paramsString, relsStatistics);
    }

private:
    catalog::RelMultiplicity relMultiplicity;
    std::vector<std::pair<common::table_id_t, common::table_id_t>> srcDstTableIDs;
    storage::RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace kuzu
