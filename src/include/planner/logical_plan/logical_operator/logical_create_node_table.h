#pragma once

#include "base_logical_operator.h"
#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateNodeTable : public LogicalDDL {

public:
    LogicalCreateNodeTable(string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        uint32_t primaryKeyIdx)
        : LogicalDDL{move(tableName), move(propertyNameDataTypes)}, primaryKeyIdx{primaryKeyIdx} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_CREATE_NODE_TABLE;
    }

    inline uint32_t getPrimaryKeyIdx() const { return primaryKeyIdx; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateNodeTable>(tableName, propertyNameDataTypes, primaryKeyIdx);
    }

private:
    uint32_t primaryKeyIdx;
};

} // namespace planner
} // namespace kuzu
