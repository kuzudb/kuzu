#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/node_expression.h"
#include "src/common/include/join_type.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

class LogicalHashJoin : public LogicalOperator {
public:
    // Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
    LogicalHashJoin(shared_ptr<NodeExpression> joinNode, JoinType joinType,
        unique_ptr<Schema> buildSideSchema, vector<uint64_t> flatOutputGroupPositions,
        expression_vector expressionsToMaterialize, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{std::move(probeSideChild), std::move(buildSideChild)},
          joinNode(std::move(joinNode)), joinType{joinType}, buildSideSchema(move(buildSideSchema)),
          flatOutputGroupPositions{move(flatOutputGroupPositions)}, expressionsToMaterialize{move(
                                                                        expressionsToMaterialize)} {
    }

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }
    inline string getExpressionsForPrinting() const override { return joinNode->getRawName(); }
    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }
    inline shared_ptr<NodeExpression> getJoinNode() const { return joinNode; }
    inline JoinType getJoinType() const { return joinType; }
    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNode, joinType, buildSideSchema->copy(),
            flatOutputGroupPositions, expressionsToMaterialize, children[0]->copy(),
            children[1]->copy());
    }

private:
    shared_ptr<NodeExpression> joinNode;
    JoinType joinType;
    unique_ptr<Schema> buildSideSchema;
    // TODO(Xiyang): solve this with issue 606
    vector<uint64_t> flatOutputGroupPositions;
    expression_vector expressionsToMaterialize;
};

} // namespace planner
} // namespace graphflow
