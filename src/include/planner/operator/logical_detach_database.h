#pragma once

#include "planner/operator/logical_database.h"

namespace kuzu {
namespace planner {

class LogicalDetachDatabase final : public LogicalDatabase {
public:
    explicit LogicalDetachDatabase(std::string dbName)
        : LogicalDatabase{LogicalOperatorType::DETACH_DATABASE, std::move(dbName)} {}

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalDetachDatabase>(dbName);
    }
};

} // namespace planner
} // namespace kuzu
