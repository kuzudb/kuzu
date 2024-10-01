#pragma once

#include "logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalSemiMaskDependency : public LogicalOperator {
public:
    explicit LogicalSemiMaskDependency(const logical_op_vector_t& children)
        : LogicalOperator{LogicalOperatorType::SEMI_MASK_DEPENDENCY, children} {
        KU_ASSERT(children.size() > 1);
    }

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override { return ""; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalSemiMaskDependency>(LogicalOperator::copy(children));
    }
};

} // namespace planner
} // namespace kuzu
