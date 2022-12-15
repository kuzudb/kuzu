#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalProjection : public LogicalOperator {
public:
    explicit LogicalProjection(expression_vector expressions, vector<uint32_t> expressionsOutputPos,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::PROJECTION, std::move(child)},
          expressions{std::move(expressions)}, expressionsOutputPos{
                                                   std::move(expressionsOutputPos)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressions);
    }

    inline expression_vector getExpressionsToProject() const { return expressions; }

    unordered_set<uint32_t> getDiscardedGroupsPos() const;

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalProjection>(
            expressions, expressionsOutputPos, children[0]->copy());
    }

private:
    expression_vector expressions;
    vector<uint32_t> expressionsOutputPos;
};

} // namespace planner
} // namespace kuzu
