#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalProjection : public LogicalOperator {
public:
    explicit LogicalProjection(expression_vector expressions,
        unordered_set<uint32_t> discardedGroupsPos, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expressions{move(expressions)}, discardedGroupsPos{move(
                                                                            discardedGroupsPos)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_PROJECTION;
    }

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToProject() const { return expressions; }

    inline unordered_set<uint32_t> getDiscardedGroupsPos() const { return discardedGroupsPos; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalProjection>(expressions, discardedGroupsPos, children[0]->copy());
    }

private:
    expression_vector expressions;
    unordered_set<uint32_t> discardedGroupsPos;
};

} // namespace planner
} // namespace kuzu
