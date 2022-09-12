#pragma once

#include "src/binder/bound_ddl/include/bound_create_table.h"

namespace graphflow {
namespace binder {

class BoundCreateRelClause : public BoundCreateTable {
public:
    explicit BoundCreateRelClause(string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        SrcDstTableIDs srcDstTableIDs)
        : BoundCreateTable{StatementType::CREATE_REL_CLAUSE, move(tableName),
              move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{move(srcDstTableIDs)} {}

    RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    SrcDstTableIDs getSrcDstTableIDs() const { return srcDstTableIDs; }

private:
    RelMultiplicity relMultiplicity;
    SrcDstTableIDs srcDstTableIDs;
};

} // namespace binder
} // namespace graphflow
