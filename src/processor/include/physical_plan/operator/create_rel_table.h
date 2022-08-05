#pragma once

#include "ddl.h"

namespace graphflow {
namespace processor {

class CreateRelTable : public DDL {

public:
    CreateRelTable(Catalog* catalog, string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        SrcDstLabels srcDstLabels, uint32_t id, const string& paramsString)
        : DDL{catalog, move(labelName), move(propertyNameDataTypes), id, paramsString},
          relMultiplicity{relMultiplicity}, srcDstLabels{move(srcDstLabels)} {}

    PhysicalOperatorType getOperatorType() override { return CREATE_REL_TABLE; }

    bool getNextTuples() override {
        catalog->addRelLabel(labelName, relMultiplicity, propertyNameDataTypes, srcDstLabels);
        return false;
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTable>(catalog, labelName, propertyNameDataTypes,
            relMultiplicity, srcDstLabels, id, paramsString);
    }

private:
    RelMultiplicity relMultiplicity;
    SrcDstLabels srcDstLabels;
};

} // namespace processor
} // namespace graphflow
