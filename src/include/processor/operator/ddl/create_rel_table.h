#pragma once

#include "processor/operator/ddl/create_table.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

class CreateRelTable : public CreateTable {
public:
    CreateRelTable(Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        vector<pair<table_id_t, table_id_t>> srcDstTableIDs, uint32_t id,
        const string& paramsString, RelsStatistics* relsStatistics)
        : CreateTable{PhysicalOperatorType::CREATE_REL_TABLE, catalog, std::move(tableName),
              std::move(propertyNameDataTypes), id, paramsString},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{std::move(srcDstTableIDs)},
          relsStatistics{relsStatistics} {}

    string execute() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTable>(catalog, tableName, propertyNameDataTypes,
            relMultiplicity, srcDstTableIDs, id, paramsString, relsStatistics);
    }

private:
    RelMultiplicity relMultiplicity;
    vector<pair<table_id_t, table_id_t>> srcDstTableIDs;
    RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace kuzu
