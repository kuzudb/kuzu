#pragma once

#include "logical_database.h"

namespace kuzu {
namespace planner {

class LogicalDetachDatabase final : public LogicalDatabase {
public:
    explicit LogicalDetachDatabase(std::string dbName,
        std::shared_ptr<binder::Expression> outputExpression,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalDatabase{LogicalOperatorType::DETACH_DATABASE, outputExpression, std::move(dbName),
              std::move(printInfo)} {}

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalDetachDatabase>(dbName, outputExpression, printInfo->copy());
    }
};

} // namespace planner
} // namespace kuzu
