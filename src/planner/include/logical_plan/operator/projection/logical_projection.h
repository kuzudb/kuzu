#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalProjection : public LogicalOperator {

public:
    explicit LogicalProjection(vector<shared_ptr<Expression>> expressions,
        unordered_set<uint32_t> discardedGroupsPos, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expressionsToProject{move(expressions)},
          discardedGroupsPos{move(discardedGroupsPos)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_PROJECTION;
    }

    string getExpressionsForPrinting() const override {
        auto result = expressionsToProject[0]->getUniqueName();
        for (auto i = 1u; i < expressionsToProject.size(); ++i) {
            result += ", " + expressionsToProject[i]->getUniqueName();
        }
        return result;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalProjection>(
            expressionsToProject, discardedGroupsPos, children[0]->copy());
    }

public:
    vector<shared_ptr<Expression>> expressionsToProject;
    unordered_set<uint32_t> discardedGroupsPos;
};

} // namespace planner
} // namespace graphflow
