#pragma once

#include "ddl.h"

namespace graphflow {
namespace processor {

class CreateRelTable : public DDL {

public:
    CreateRelTable(Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        SrcDstTableIDs srcDstTableIDs, uint32_t id, const string& paramsString,
        RelsStatistics* relsStatistics)
        : DDL{catalog, move(tableName), move(propertyNameDataTypes), id, paramsString},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{move(srcDstTableIDs)},
          relsStatistics{relsStatistics} {}

    PhysicalOperatorType getOperatorType() override { return CREATE_REL_TABLE; }

    bool getNextTuples() override {
        auto newRelTableID = catalog->addRelTableSchema(
            tableName, relMultiplicity, propertyNameDataTypes, srcDstTableIDs);
        relsStatistics->addTableStatistic(
            catalog->getWriteVersion()->getRelTableSchema(newRelTableID));
        return false;
    }

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
