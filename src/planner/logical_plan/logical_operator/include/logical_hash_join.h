#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

class LogicalHashJoin : public LogicalOperator {

public:
    // Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
    LogicalHashJoin(shared_ptr<NodeExpression> joinNode, unique_ptr<Schema> buildSideSchema,
        vector<uint64_t> flatOutputGroupPositions, expression_vector expressionsToMaterialize,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{move(probeSideChild), move(buildSideChild)}, joinNode(move(joinNode)),
          buildSideSchema(move(buildSideSchema)), flatOutputGroupPositions{move(
                                                      flatOutputGroupPositions)},
          expressionsToMaterialize{move(expressionsToMaterialize)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }

    string getExpressionsForPrinting() const override { return joinNode->getRawName(); }

    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }

    inline shared_ptr<NodeExpression> getJoinNode() const { return joinNode; }
    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNode, buildSideSchema->copy(),
            flatOutputGroupPositions, expressionsToMaterialize, children[0]->copy(),
            children[1]->copy());
    }

private:
    shared_ptr<NodeExpression> joinNode;
    unique_ptr<Schema> buildSideSchema;
    // TODO(Xiyang): solve this with issue 606
    vector<uint64_t> flatOutputGroupPositions;
    expression_vector expressionsToMaterialize;
};
} // namespace planner
} // namespace graphflow
