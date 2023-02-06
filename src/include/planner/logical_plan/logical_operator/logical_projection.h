#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalProjection : public LogicalOperator {
public:
    explicit LogicalProjection(binder::expression_vector expressions,
        std::vector<uint32_t> expressionsOutputPos, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::PROJECTION, std::move(child)},
          expressions{std::move(expressions)}, expressionsOutputPos{
                                                   std::move(expressionsOutputPos)} {}

    void computeSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressions);
    }

    inline binder::expression_vector getExpressionsToProject() const { return expressions; }

    std::unordered_set<uint32_t> getDiscardedGroupsPos() const;

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalProjection>(
            expressions, expressionsOutputPos, children[0]->copy());
    }

private:
    binder::expression_vector expressions;
    std::vector<uint32_t> expressionsOutputPos;
};

} // namespace planner
} // namespace kuzu
