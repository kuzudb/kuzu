#pragma once

#include "logical_create_table.h"

namespace kuzu {
namespace planner {

class LogicalCreateNodeTable : public LogicalCreateTable {
public:
    LogicalCreateNodeTable(string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        uint32_t primaryKeyIdx)
        : LogicalCreateTable{LogicalOperatorType::CREATE_NODE_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          primaryKeyIdx{primaryKeyIdx} {}

    inline uint32_t getPrimaryKeyIdx() const { return primaryKeyIdx; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateNodeTable>(tableName, propertyNameDataTypes, primaryKeyIdx);
    }

private:
    uint32_t primaryKeyIdx;
};

} // namespace planner
} // namespace kuzu
