#pragma once

#include "logical_create_table.h"

namespace kuzu {
namespace planner {

class LogicalCreateNodeTable : public LogicalCreateTable {
public:
    LogicalCreateNodeTable(std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes, uint32_t primaryKeyIdx,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalCreateTable{LogicalOperatorType::CREATE_NODE_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes), std::move(outputExpression)},
          primaryKeyIdx{primaryKeyIdx} {}

    inline uint32_t getPrimaryKeyIdx() const { return primaryKeyIdx; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateNodeTable>(
            tableName, propertyNameDataTypes, primaryKeyIdx, outputExpression);
    }

private:
    uint32_t primaryKeyIdx;
};

} // namespace planner
} // namespace kuzu
