#pragma once

#include "logical_database.h"

namespace kuzu {
namespace planner {

class LogicalDetachDatabase final : public LogicalDatabase {
public:
    explicit LogicalDetachDatabase(std::string dbName,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDatabase{LogicalOperatorType::DETACH_DATABASE, outputExpression,
              std::move(dbName)} {}

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalDetachDatabase>(dbName, outputExpression);
    }
};

} // namespace planner
} // namespace kuzu
