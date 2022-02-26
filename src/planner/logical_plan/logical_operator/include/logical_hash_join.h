#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace graphflow {
namespace planner {

class LogicalHashJoin : public LogicalOperator {

public:
    // Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
    LogicalHashJoin(string joinNodeID, unique_ptr<Schema> buildSideSchema,
        expression_vector expressionsToMaterialize, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{move(probeSideChild), move(buildSideChild)}, joinNodeID(move(joinNodeID)),
          buildSideSchema(move(buildSideSchema)), expressionsToMaterialize{
                                                      move(expressionsToMaterialize)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }

    string getExpressionsForPrinting() const override { return joinNodeID; }

    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }

    inline string getJoinNodeID() const { return joinNodeID; }

    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNodeID, buildSideSchema->copy(),
            expressionsToMaterialize, children[0]->copy(), children[1]->copy());
    }

private:
    string joinNodeID;
    unique_ptr<Schema> buildSideSchema;
    expression_vector expressionsToMaterialize;
};
} // namespace planner
} // namespace graphflow
