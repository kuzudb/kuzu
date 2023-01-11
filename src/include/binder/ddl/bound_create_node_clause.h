#pragma once

#include "bound_create_table.h"

namespace kuzu {
namespace binder {

class BoundCreateNodeClause : public BoundCreateTable {
public:
    explicit BoundCreateNodeClause(string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, uint32_t primaryKeyIdx)
        : BoundCreateTable{StatementType::CREATE_NODE_CLAUSE, move(tableName),
              move(propertyNameDataTypes)},
          primaryKeyIdx{primaryKeyIdx} {}

    inline uint32_t getPrimaryKeyIdx() const { return primaryKeyIdx; }

private:
    uint32_t primaryKeyIdx;
};

} // namespace binder
} // namespace kuzu
