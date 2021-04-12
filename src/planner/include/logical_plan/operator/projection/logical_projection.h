#pragma once

#include "src/expression/include/logical/logical_expression.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class LogicalProjection : public LogicalOperator {

public:
    explicit LogicalProjection(
        vector<shared_ptr<LogicalExpression>> expressions, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, expressionsToProject{move(expressions)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_PROJECTION;
    }

    string getOperatorInformation() const override {
        auto result = expressionsToProject[0]->getRawExpression();
        for (auto i = 1u; i < expressionsToProject.size(); ++i) {
            result += ", " + expressionsToProject[i]->getRawExpression();
        }
        return result;
    }

public:
    vector<shared_ptr<LogicalExpression>> expressionsToProject;
};

} // namespace planner
} // namespace graphflow
