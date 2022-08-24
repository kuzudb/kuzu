#pragma once

#include "ddl.h"

namespace graphflow {
namespace processor {

class CreateRelTable : public DDL {

public:
    CreateRelTable(Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        SrcDstTableIDs srcDstTableIDs, uint32_t id, const string& paramsString)
        : DDL{catalog, move(tableName), move(propertyNameDataTypes), id, paramsString},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{move(srcDstTableIDs)} {}

    PhysicalOperatorType getOperatorType() override { return CREATE_REL_TABLE; }

    bool getNextTuples() override {
        catalog->addRelTableSchema(
            tableName, relMultiplicity, propertyNameDataTypes, srcDstTableIDs);
        return false;
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTable>(catalog, tableName, propertyNameDataTypes,
            relMultiplicity, srcDstTableIDs, id, paramsString);
    }

private:
    RelMultiplicity relMultiplicity;
    SrcDstTableIDs srcDstTableIDs;
};

} // namespace processor
} // namespace graphflow
