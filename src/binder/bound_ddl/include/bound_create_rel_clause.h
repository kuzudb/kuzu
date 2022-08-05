#pragma once

#include "src/binder/bound_ddl/include/bound_ddl.h"
#include "src/catalog/include/catalog_structs.h"

namespace graphflow {
namespace binder {

class BoundCreateRelClause : public BoundDDL {
public:
    explicit BoundCreateRelClause(string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        SrcDstLabels srcDstLabels)
        : BoundDDL{StatementType::CREATE_REL_CLAUSE, move(labelName), move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstLabels{move(srcDstLabels)} {}

    RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    SrcDstLabels getSrcDstLabels() const { return srcDstLabels; }

private:
    RelMultiplicity relMultiplicity;
    SrcDstLabels srcDstLabels;
};

} // namespace binder
} // namespace graphflow
