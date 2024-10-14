#pragma once

#include "binder/expression/expression.h"
#include "binder/expression/expression_util.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalProjection : public LogicalOperator {
public:
    explicit LogicalProjection(binder::expression_vector expressions,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::PROJECTION, std::move(child)},
          expressions{std::move(expressions)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressions);
    }

    inline binder::expression_vector getExpressionsToProject() const { return expressions; }

    std::unordered_set<uint32_t> getDiscardedGroupsPos() const;

    std::unique_ptr<LogicalOperator> copy() override {
        auto op = make_unique<LogicalProjection>(expressions, children[0]->copy());
        return op;
    }

private:
    binder::expression_vector expressions;
};

} // namespace planner
} // namespace kuzu
