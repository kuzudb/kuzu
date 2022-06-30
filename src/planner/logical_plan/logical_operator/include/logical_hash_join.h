#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace graphflow {
namespace planner {

class LogicalHashJoin : public LogicalOperator {

public:
    // Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
    LogicalHashJoin(string joinNodeID, unique_ptr<Schema> buildSideSchema,
        vector<uint64_t> flatOutputGroupPositions, expression_vector expressionsToMaterialize,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{move(probeSideChild), move(buildSideChild)}, joinNodeID(move(joinNodeID)),
          buildSideSchema(move(buildSideSchema)), flatOutputGroupPositions{move(
                                                      flatOutputGroupPositions)},
          expressionsToMaterialize{move(expressionsToMaterialize)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }

    string getExpressionsForPrinting() const override { return joinNodeID; }

    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }

    inline string getJoinNodeID() const { return joinNodeID; }

    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNodeID, buildSideSchema->copy(),
            flatOutputGroupPositions, expressionsToMaterialize, children[0]->copy(),
            children[1]->copy());
    }

private:
    string joinNodeID;
    unique_ptr<Schema> buildSideSchema;
    // TODO(Xiyang): solve this with issue 606
    vector<uint64_t> flatOutputGroupPositions;
    expression_vector expressionsToMaterialize;
};
} // namespace planner
} // namespace graphflow
