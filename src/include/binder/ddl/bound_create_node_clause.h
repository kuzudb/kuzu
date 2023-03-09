#pragma once

#include "bound_create_table.h"

namespace kuzu {
namespace binder {

class BoundCreateNodeClause : public BoundCreateTable {
public:
    explicit BoundCreateNodeClause(std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes, uint32_t primaryKeyIdx)
        : BoundCreateTable{common::StatementType::CREATE_NODE_CLAUSE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          primaryKeyIdx{primaryKeyIdx} {}

    inline uint32_t getPrimaryKeyIdx() const { return primaryKeyIdx; }

private:
    uint32_t primaryKeyIdx;
};

} // namespace binder
} // namespace kuzu
