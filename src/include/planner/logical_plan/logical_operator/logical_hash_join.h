#pragma once

#include <utility>

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "common/join_type.h"
#include "schema.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

// Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
class LogicalHashJoin : public LogicalOperator {
public:
    // Inner and left join.
    LogicalHashJoin(vector<shared_ptr<NodeExpression>> joinNodes, JoinType joinType,
        bool isProbeAcc, unique_ptr<Schema> buildSideSchema,
        vector<uint64_t> flatOutputGroupPositions, expression_vector expressionsToMaterialize,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNodes), joinType, nullptr, isProbeAcc,
              std::move(buildSideSchema), std::move(flatOutputGroupPositions),
              std::move(expressionsToMaterialize), std::move(probeSideChild),
              std::move(buildSideChild)} {}

    // Mark join.
    LogicalHashJoin(vector<shared_ptr<NodeExpression>> joinNodes, shared_ptr<Expression> mark,
        bool isProbeAcc, unique_ptr<Schema> buildSideSchema,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNodes), JoinType::MARK, std::move(mark), isProbeAcc,
              std::move(buildSideSchema), vector<uint64_t>{} /* flatOutputGroupPositions */,
              expression_vector{} /* expressionsToMaterialize */, std::move(probeSideChild),
              std::move(buildSideChild)} {}

    LogicalHashJoin(vector<shared_ptr<NodeExpression>> joinNodes, JoinType joinType,
        shared_ptr<Expression> mark, bool isProbeAcc, unique_ptr<Schema> buildSideSchema,
        vector<uint64_t> flatOutputGroupPositions, expression_vector expressionsToMaterialize,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{std::move(probeSideChild), std::move(buildSideChild)},
          joinNodes(std::move(joinNodes)), joinType{joinType}, mark{std::move(mark)},
          isProbeAcc{isProbeAcc},
          buildSideSchema(std::move(buildSideSchema)), flatOutputGroupPositions{std::move(
                                                           flatOutputGroupPositions)},
          expressionsToMaterialize{std::move(expressionsToMaterialize)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }
    inline string getExpressionsForPrinting() const override {
        string result;
        for (auto i = 0u; i < joinNodes.size() - 1; i++) {
            result += joinNodes[i]->getRawName() + ",";
        }
        result += joinNodes[joinNodes.size() - 1]->getRawName();
        return result;
    }
    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }
    inline vector<shared_ptr<NodeExpression>> getJoinNodes() const { return joinNodes; }
    inline JoinType getJoinType() const { return joinType; }

    inline shared_ptr<Expression> getMark() const {
        assert(joinType == JoinType::MARK && mark);
        return mark;
    }
    inline bool getIsProbeAcc() const { return isProbeAcc; }
    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNodes, joinType, mark, isProbeAcc,
            buildSideSchema->copy(), flatOutputGroupPositions, expressionsToMaterialize,
            children[0]->copy(), children[1]->copy());
    }

private:
    vector<shared_ptr<NodeExpression>> joinNodes;
    JoinType joinType;
    shared_ptr<Expression> mark; // when joinType is Mark
    bool isProbeAcc;
    unique_ptr<Schema> buildSideSchema;
    // TODO(Xiyang): solve this with issue 606
    vector<uint64_t> flatOutputGroupPositions;
    expression_vector expressionsToMaterialize;
};

} // namespace planner
} // namespace kuzu
