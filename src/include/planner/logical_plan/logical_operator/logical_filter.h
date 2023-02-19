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

class LogicalFilterFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(LogicalFilter* filter) {
        return getGroupsPosToFlatten(filter->getPredicate(), filter->getChild(0).get());
    }
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(
        const std::shared_ptr<binder::Expression>& expression, LogicalOperator* filterChild);
};

} // namespace planner
} // namespace kuzu
