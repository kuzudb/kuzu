#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace planner {

class LogicalProjection : public LogicalOperator {

public:
    explicit LogicalProjection(
        vector<shared_ptr<Expression>> expressions, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, expressionsToProject{move(expressions)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_PROJECTION;
    }

    string getOperatorInformation() const override {
        auto result = expressionsToProject[0]->getAliasElseRawExpression();
        for (auto i = 1u; i < expressionsToProject.size(); ++i) {
            result += ", " + expressionsToProject[i]->getAliasElseRawExpression();
        }
        return result;
    }

public:
    vector<shared_ptr<Expression>> expressionsToProject;
};

} // namespace planner
} // namespace graphflow
