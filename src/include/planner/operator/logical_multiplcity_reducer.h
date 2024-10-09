#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalMultiplicityReducer : public LogicalOperator {
public:
    explicit LogicalMultiplicityReducer(std::shared_ptr<LogicalOperator> child,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator(LogicalOperatorType::MULTIPLICITY_REDUCER, std::move(child),
              std::move(printInfo)) {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalMultiplicityReducer>(children[0]->copy(), printInfo->copy());
    }
};

} // namespace planner
} // namespace kuzu
