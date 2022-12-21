#pragma once

#include <utility>

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "common/join_type.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

// Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
class LogicalHashJoin : public LogicalOperator {
public:
    // Inner and left join.
    LogicalHashJoin(vector<shared_ptr<NodeExpression>> joinNodes, JoinType joinType,
        bool isProbeAcc, expression_vector expressionsToMaterialize,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNodes), joinType, nullptr, UINT32_MAX, isProbeAcc,
              std::move(expressionsToMaterialize), std::move(probeSideChild),
              std::move(buildSideChild)} {}

    // Mark join.
    LogicalHashJoin(vector<shared_ptr<NodeExpression>> joinNodes, shared_ptr<Expression> mark,
        uint32_t markPos, bool isProbeAcc, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNodes), JoinType::MARK, std::move(mark), markPos,
              isProbeAcc, expression_vector{} /* expressionsToMaterialize */,
              std::move(probeSideChild), std::move(buildSideChild)} {}

    LogicalHashJoin(vector<shared_ptr<NodeExpression>> joinNodes, JoinType joinType,
        shared_ptr<Expression> mark, uint32_t markPos, bool isProbeAcc,
        expression_vector expressionsToMaterialize, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{LogicalOperatorType::HASH_JOIN, std::move(probeSideChild),
              std::move(buildSideChild)},
          joinNodes(std::move(joinNodes)), joinType{joinType}, mark{std::move(mark)},
          markPos{markPos}, isProbeAcc{isProbeAcc}, expressionsToMaterialize{
                                                        std::move(expressionsToMaterialize)} {}

    void computeSchema() override;

    string getExpressionsForPrinting() const override;

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
    inline Schema* getBuildSideSchema() const { return children[1]->getSchema(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNodes, joinType, mark, markPos, isProbeAcc,
            expressionsToMaterialize, children[0]->copy(), children[1]->copy());
    }

private:
    vector<shared_ptr<NodeExpression>> joinNodes;
    JoinType joinType;
    shared_ptr<Expression> mark; // when joinType is Mark
    uint32_t markPos;
    bool isProbeAcc;
    expression_vector expressionsToMaterialize;
};

} // namespace planner
} // namespace kuzu
