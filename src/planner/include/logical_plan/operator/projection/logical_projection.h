#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalProjection : public LogicalOperator {

public:
    explicit LogicalProjection(vector<shared_ptr<Expression>> expressions,
        unique_ptr<Schema> schemaBeforeProjection, vector<uint32_t> discardedGroupPos,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, expressionsToProject{move(expressions)},
          schemaBeforeProjection{move(schemaBeforeProjection)},
          discardedGroupPos(move(discardedGroupPos)) {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_PROJECTION;
    }

    string getExpressionsForPrinting() const override {
        auto result = expressionsToProject[0]->getInternalName();
        for (auto i = 1u; i < expressionsToProject.size(); ++i) {
            result += ", " + expressionsToProject[i]->getInternalName();
        }
        return result;
    }

public:
    vector<shared_ptr<Expression>> expressionsToProject;
    unique_ptr<Schema> schemaBeforeProjection;
    vector<uint32_t> discardedGroupPos;
};

} // namespace planner
} // namespace graphflow
