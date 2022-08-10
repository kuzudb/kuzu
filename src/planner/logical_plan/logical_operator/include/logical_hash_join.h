#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

enum class HashJoinType : uint8_t { HASH_JOIN, S_JOIN, ASP_JOIN };

class LogicalHashJoin : public LogicalOperator {
public:
    // Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
    LogicalHashJoin(shared_ptr<NodeExpression> joinNode, unique_ptr<Schema> buildSideSchema,
        vector<uint64_t> flatOutputGroupPositions, expression_vector expressionsToMaterialize,
        bool isOutputAFlatTuple, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{move(probeSideChild), move(buildSideChild)}, joinNode(move(joinNode)),
          buildSideSchema(move(buildSideSchema)),
          flatOutputGroupPositions{move(flatOutputGroupPositions)}, expressionsToMaterialize{move(
                                                                        expressionsToMaterialize)},
          isOutputAFlatTuple{isOutputAFlatTuple}, joinType{HashJoinType::HASH_JOIN} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }

    string getExpressionsForPrinting() const override {
        switch (joinType) {
        case HashJoinType::ASP_JOIN:
        case HashJoinType::S_JOIN: {
            return "SEMI " + joinNode->getRawName();
        }
        default:
            return joinNode->getRawName();
        }
    }

    inline shared_ptr<NodeExpression> getJoinNode() const { return joinNode; }
    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }
    inline bool getIsOutputAFlatTuple() const { return isOutputAFlatTuple; }

    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }
    inline void setJoinType(HashJoinType hashJoinType) { joinType = hashJoinType; }
    inline HashJoinType getJoinType() const { return joinType; }

    unique_ptr<LogicalOperator> copy() override {
        auto copiedHashJoin = make_unique<LogicalHashJoin>(joinNode, buildSideSchema->copy(),
            flatOutputGroupPositions, expressionsToMaterialize, isOutputAFlatTuple,
            children[0]->copy(), children[1]->copy());
        copiedHashJoin->setJoinType(joinType);
        return copiedHashJoin;
    }

private:
    shared_ptr<NodeExpression> joinNode;
    unique_ptr<Schema> buildSideSchema;
    // TODO(Xiyang): solve this with issue 606
    vector<uint64_t> flatOutputGroupPositions;
    expression_vector expressionsToMaterialize;
    bool isOutputAFlatTuple;
    HashJoinType joinType;
};

} // namespace planner
} // namespace graphflow
