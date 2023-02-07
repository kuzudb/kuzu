#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalFilter : public LogicalOperator {
public:
    LogicalFilter(std::shared_ptr<binder::Expression> expression, uint32_t groupPosToSelect,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FILTER, std::move(child)},
          expression{std::move(expression)}, groupPosToSelect{groupPosToSelect} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        return expression->getRawName();
    }

    inline std::shared_ptr<binder::Expression> getPredicate() const { return expression; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFilter>(expression, groupPosToSelect, children[0]->copy());
    }

public:
    std::shared_ptr<binder::Expression> expression;
    uint32_t groupPosToSelect;
};

} // namespace planner
} // namespace kuzu
