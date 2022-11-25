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
        : CreateTable{catalog, move(tableName), move(propertyNameDataTypes), id, paramsString},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{move(srcDstTableIDs)},
          relsStatistics{relsStatistics} {}

    PhysicalOperatorType getOperatorType() override { return CREATE_REL_TABLE; }

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
