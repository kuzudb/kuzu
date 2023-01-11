#pragma once

#include "bound_create_table.h"

namespace kuzu {
namespace binder {

class BoundCreateRelClause : public BoundCreateTable {
public:
    explicit BoundCreateRelClause(string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        vector<pair<table_id_t, table_id_t>> srcDstTableIDs)
        : BoundCreateTable{StatementType::CREATE_REL_CLAUSE, move(tableName),
              move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{move(srcDstTableIDs)} {}

    RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    vector<pair<table_id_t, table_id_t>> getSrcDstTableIDs() const { return srcDstTableIDs; }

private:
    RelMultiplicity relMultiplicity;
    vector<pair<table_id_t, table_id_t>> srcDstTableIDs;
};

} // namespace binder
} // namespace kuzu
