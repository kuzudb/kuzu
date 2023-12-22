#pragma once

#include "logical_operator.h"

namespace kuzu {
namespace planner {

// Avoid executing if a sub-plan is known to have empty result (e.g. WHERE False)
// See FilterPushDownOptimizer::visitFilterReplace for more details on the optimization.
class LogicalEmptyResult final : public LogicalOperator {
public:
    explicit LogicalEmptyResult(const Schema& schema)
        : LogicalOperator{LogicalOperatorType::EMPTY_RESULT} {
        this->schema = schema.copy();
    }

    void computeFactorizedSchema() override {}
    void computeFlatSchema() override {}

    std::string getExpressionsForPrinting() const override { return std::string{}; }

    inline std::unique_ptr<LogicalOperator> copy() override { KU_UNREACHABLE; }
};

} // namespace planner
} // namespace kuzu
