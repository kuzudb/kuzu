#pragma once

#include "logical_simple.h"

namespace kuzu {
namespace planner {

class LogicalImportDatabase : public LogicalSimple {
public:
    LogicalImportDatabase(std::string query, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalSimple{LogicalOperatorType::IMPORT_DATABASE, std::move(outputExpression)},
          query{query} {}

    std::string getQuery() const { return query; }

    std::string getExpressionsForPrinting() const override { return std::string{}; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalImportDatabase>(query, outputExpression);
    }

private:
    // see comment in BoundImportDatabase
    std::string query;
};

} // namespace planner
} // namespace kuzu
