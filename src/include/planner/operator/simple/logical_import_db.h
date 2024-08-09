#pragma once

#include "logical_simple.h"

namespace kuzu {
namespace planner {

class LogicalImportDatabase : public LogicalSimple {
public:
    LogicalImportDatabase(std::string query, std::shared_ptr<binder::Expression> outputExpression,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalSimple{LogicalOperatorType::IMPORT_DATABASE, std::move(outputExpression),
              std::move(printInfo)},
          query{query} {}

    std::string getQuery() const { return query; }

    std::string getExpressionsForPrinting() const override { return std::string{}; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalImportDatabase>(query, outputExpression, printInfo->copy());
    }

private:
    // see comment in BoundImportDatabase
    std::string query;
};

} // namespace planner
} // namespace kuzu
