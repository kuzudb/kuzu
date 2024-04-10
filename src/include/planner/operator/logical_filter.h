#pragma once

#include "binder/expression/expression.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalFilter : public LogicalOperator {
public:
    LogicalFilter(std::shared_ptr<binder::Expression> expression,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FILTER, std::move(child)},
          expression{std::move(expression)} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    f_group_pos_set getGroupsPosToFlatten();

    inline std::string getExpressionsForPrinting() const override { return expression->toString(); }

    inline std::shared_ptr<binder::Expression> getPredicate() const { return expression; }

    f_group_pos getGroupPosToSelect() const;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFilter>(expression, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> expression;
};

} // namespace planner
} // namespace kuzu
