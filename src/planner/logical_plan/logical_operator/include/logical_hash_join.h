#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/node_expression.h"
#include "src/common/include/join_type.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

// Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
class LogicalHashJoin : public LogicalOperator {
public:
    // Inner and left join.
    LogicalHashJoin(shared_ptr<NodeExpression> joinNode, JoinType joinType, bool isProbeAcc,
        unique_ptr<Schema> buildSideSchema, vector<uint64_t> flatOutputGroupPositions,
        expression_vector expressionsToMaterialize, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNode), joinType, nullptr, isProbeAcc,
              std::move(buildSideSchema), std::move(flatOutputGroupPositions),
              std::move(expressionsToMaterialize), std::move(probeSideChild),
              std::move(buildSideChild)} {}

    // Mark join.
    LogicalHashJoin(shared_ptr<NodeExpression> joinNode, shared_ptr<Expression> mark,
        unique_ptr<Schema> buildSideSchema, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNode), JoinType::MARK, mark, false /* isProbeAcc */,
              std::move(buildSideSchema), vector<uint64_t>{} /* flatOutputGroupPositions */,
              expression_vector{} /* expressionsToMaterialize */, std::move(probeSideChild),
              std::move(buildSideChild)} {}

    LogicalHashJoin(shared_ptr<NodeExpression> joinNode, JoinType joinType,
        shared_ptr<Expression> mark, bool isProbeAcc, unique_ptr<Schema> buildSideSchema,
        vector<uint64_t> flatOutputGroupPositions, expression_vector expressionsToMaterialize,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{std::move(probeSideChild), std::move(buildSideChild)},
          joinNode(std::move(joinNode)), joinType{joinType}, mark{std::move(mark)},
          isProbeAcc{isProbeAcc},
          buildSideSchema(std::move(buildSideSchema)), flatOutputGroupPositions{std::move(
                                                           flatOutputGroupPositions)},
          expressionsToMaterialize{std::move(expressionsToMaterialize)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }
    inline string getExpressionsForPrinting() const override { return joinNode->getRawName(); }
    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }
    inline shared_ptr<NodeExpression> getJoinNode() const { return joinNode; }
    inline JoinType getJoinType() const { return joinType; }

    inline shared_ptr<Expression> getMark() const {
        assert(joinType == JoinType::MARK);
        return mark;
    }
    inline bool getIsProbeAcc() const { return isProbeAcc; }
    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNode, joinType, mark, isProbeAcc,
            buildSideSchema->copy(), flatOutputGroupPositions, expressionsToMaterialize,
            children[0]->copy(), children[1]->copy());
    }

protected:
    shared_ptr<NodeExpression> joinNode;
    JoinType joinType;
    shared_ptr<Expression> mark; // when joinType is Mark
    bool isProbeAcc;
    unique_ptr<Schema> buildSideSchema;
    // TODO(Xiyang): solve this with issue 606
    vector<uint64_t> flatOutputGroupPositions;
    expression_vector expressionsToMaterialize;
};

} // namespace planner
} // namespace graphflow
