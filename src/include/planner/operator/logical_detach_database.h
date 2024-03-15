#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDetachDatabase final : public LogicalOperator {
public:
    explicit LogicalDetachDatabase(std::string dbName)
        : LogicalOperator{LogicalOperatorType::DETACH_DATABASE}, dbName{std::move(dbName)} {}

    std::string getDBName() const { return dbName; }

    std::string getExpressionsForPrinting() const override { return dbName; }

    void computeFactorizedSchema() override { createEmptySchema(); }
    void computeFlatSchema() override { createEmptySchema(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalDetachDatabase>(dbName);
    }

private:
    std::string dbName;
};

} // namespace planner
} // namespace kuzu
