#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalFilter : public LogicalOperator {
public:
    LogicalFilter(
        std::shared_ptr<binder::Expression> expression, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FILTER, std::move(child)}, expression{std::move(
                                                                              expression)} {}

    f_group_pos_set getGroupsPosToFlatten();

    inline void computeSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        return expression->getRawName();
    }

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
