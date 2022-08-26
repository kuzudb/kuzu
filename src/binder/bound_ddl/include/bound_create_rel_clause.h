#pragma once

#include "src/binder/bound_ddl/include/bound_ddl.h"
#include "src/catalog/include/catalog_structs.h"

namespace graphflow {
namespace binder {

class BoundCreateRelClause : public BoundDDL {
public:
    explicit BoundCreateRelClause(string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        SrcDstTableIDs srcDstTableIDs)
        : BoundDDL{StatementType::CREATE_REL_CLAUSE, move(tableName), move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{move(srcDstTableIDs)} {}

    RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    SrcDstTableIDs getSrcDstTableIDs() const { return srcDstTableIDs; }

private:
    RelMultiplicity relMultiplicity;
    SrcDstTableIDs srcDstTableIDs;
};

} // namespace binder
} // namespace graphflow
