#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDDL : public LogicalOperator {
public:
    LogicalDDL(LogicalOperatorType operatorType, string tableName)
        : LogicalOperator{operatorType}, tableName{std::move(tableName)} {}

    inline string getTableName() const { return tableName; }

    inline string getExpressionsForPrinting() const override { return tableName; }

    inline void computeSchema() override { schema = make_unique<Schema>(); }

protected:
    string tableName;
};

} // namespace planner
} // namespace kuzu
