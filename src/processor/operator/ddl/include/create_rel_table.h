#pragma once

#include "src/processor/operator/ddl/include/create_table.h"
#include "src/storage/store/include/rels_statistics.h"

namespace graphflow {
namespace processor {

class CreateRelTable : public CreateTable {

public:
    CreateRelTable(Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        SrcDstTableIDs srcDstTableIDs, uint32_t id, const string& paramsString,
        RelsStatistics* relsStatistics)
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
    SrcDstTableIDs srcDstTableIDs;
    RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace graphflow
