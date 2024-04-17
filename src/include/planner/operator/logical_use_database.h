#pragma once

#include "planner/operator/logical_database.h"

namespace kuzu {
namespace planner {

class LogicalUseDatabase final : public LogicalDatabase {
public:
    explicit LogicalUseDatabase(std::string dbName)
        : LogicalDatabase{LogicalOperatorType::USE_DATABASE, std::move(dbName)} {}

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalUseDatabase>(dbName);
    }
};

} // namespace planner
} // namespace kuzu
