#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalHashJoin : public LogicalOperator {

public:
    // Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
    LogicalHashJoin(string joinNodeID, unique_ptr<Schema> buildSideSchema,
        unordered_set<string> expressionToMaterializeNames,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{move(probeSideChild), move(buildSideChild)}, joinNodeID(move(joinNodeID)),
          buildSideSchema(move(buildSideSchema)), expressionToMaterializeNames{
                                                      move(expressionToMaterializeNames)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }

    string getExpressionsForPrinting() const override { return joinNodeID; }

    inline unordered_set<string> getExpressionToMaterializeNames() const {
        return expressionToMaterializeNames;
    }

    inline void addExpressionToMaterialize(const string& name) {
        expressionToMaterializeNames.insert(name);
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNodeID, buildSideSchema->copy(),
            expressionToMaterializeNames, children[0]->copy(), children[1]->copy());
    }

public:
    const string joinNodeID;
    unique_ptr<Schema> buildSideSchema;

private:
    unordered_set<string> expressionToMaterializeNames;
};
} // namespace planner
} // namespace graphflow
