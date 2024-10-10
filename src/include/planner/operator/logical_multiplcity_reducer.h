#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalMultiplicityReducer final : public LogicalOperator {
public:
    explicit LogicalMultiplicityReducer(std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::MULTIPLICITY_REDUCER, std::move(child)) {}

    void computeFactorizedSchema() override { copyChildSchema(0); }
    void computeFlatSchema() override { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override { return std::string(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalMultiplicityReducer>(children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu
