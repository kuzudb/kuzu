#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalImportDatabase : public LogicalOperator {
public:
    explicit LogicalImportDatabase(std::string query)
        : LogicalOperator{LogicalOperatorType::IMPORT_DATABASE}, query{query} {}

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getQuery() const { return query; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalImportDatabase>(query);
    }

private:
    std::string query;
};

} // namespace planner
} // namespace kuzu
