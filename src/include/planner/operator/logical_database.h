#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDatabase : public LogicalOperator {
public:
    explicit LogicalDatabase(LogicalOperatorType operatorType, std::string dbName)
        : LogicalOperator{operatorType}, dbName{std::move(dbName)} {}

    std::string getDBName() const { return dbName; }

    std::string getExpressionsForPrinting() const override { return dbName; }

    void computeFactorizedSchema() override { createEmptySchema(); }
    void computeFlatSchema() override { createEmptySchema(); }

protected:
    std::string dbName;
};

} // namespace planner
} // namespace kuzu
