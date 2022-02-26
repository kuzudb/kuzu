#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalProjection : public LogicalOperator {

public:
    explicit LogicalProjection(expression_vector expressionsToProject,
        unordered_set<uint32_t> discardedGroupsPos, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expressionsToProject{move(expressionsToProject)},
          discardedGroupsPos{move(discardedGroupsPos)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_PROJECTION;
    }

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToProject() const { return expressionsToProject; }

    inline unordered_set<uint32_t> getDiscardedGroupsPos() const { return discardedGroupsPos; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalProjection>(
            expressionsToProject, discardedGroupsPos, children[0]->copy());
    }

private:
    expression_vector expressionsToProject;
    unordered_set<uint32_t> discardedGroupsPos;
};

} // namespace planner
} // namespace graphflow
