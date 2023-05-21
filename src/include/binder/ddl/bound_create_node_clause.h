#pragma once

#include "bound_create_table.h"

namespace kuzu {
namespace binder {

class BoundCreateNodeClause : public BoundCreateTable {
public:
    explicit BoundCreateNodeClause(
        std::string tableName, std::vector<catalog::Property> properties, uint32_t primaryKeyIdx)
        : BoundCreateTable{common::StatementType::CREATE_NODE_TABLE, std::move(tableName),
              std::move(properties)},
          primaryKeyIdx{primaryKeyIdx} {}

    inline uint32_t getPrimaryKeyIdx() const { return primaryKeyIdx; }

private:
    uint32_t primaryKeyIdx;
};

} // namespace binder
} // namespace kuzu
