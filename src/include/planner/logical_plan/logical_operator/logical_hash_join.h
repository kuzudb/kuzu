#pragma once

#include <utility>

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "common/join_type.h"

namespace kuzu {
namespace planner {

// Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
class LogicalHashJoin : public LogicalOperator {
public:
    // Inner and left join.
    LogicalHashJoin(binder::expression_vector joinNodeIDs, common::JoinType joinType,
        bool isProbeAcc, binder::expression_vector expressionsToMaterialize,
        std::shared_ptr<LogicalOperator> probeSideChild,
        std::shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNodeIDs), joinType, nullptr, UINT32_MAX, isProbeAcc,
              std::move(expressionsToMaterialize), std::move(probeSideChild),
              std::move(buildSideChild)} {}

    // Mark join.
    LogicalHashJoin(binder::expression_vector joinNodeIDs, std::shared_ptr<binder::Expression> mark,
        uint32_t markPos, bool isProbeAcc, std::shared_ptr<LogicalOperator> probeSideChild,
        std::shared_ptr<LogicalOperator> buildSideChild)
        : LogicalHashJoin{std::move(joinNodeIDs), common::JoinType::MARK, std::move(mark), markPos,
              isProbeAcc, binder::expression_vector{} /* expressionsToMaterialize */,
              std::move(probeSideChild), std::move(buildSideChild)} {}

    LogicalHashJoin(binder::expression_vector joinNodeIDs, common::JoinType joinType,
        std::shared_ptr<binder::Expression> mark, uint32_t markPos, bool isProbeAcc,
        binder::expression_vector expressionsToMaterialize,
        std::shared_ptr<LogicalOperator> probeSideChild,
        std::shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{LogicalOperatorType::HASH_JOIN, std::move(probeSideChild),
              std::move(buildSideChild)},
          joinNodeIDs(std::move(joinNodeIDs)), joinType{joinType}, mark{std::move(mark)},
          markPos{markPos}, isProbeAcc{isProbeAcc}, expressionsToMaterialize{
                                                        std::move(expressionsToMaterialize)} {}

    void computeSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(joinNodeIDs);
    }

    inline binder::expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }
    inline binder::expression_vector getJoinNodeIDs() const { return joinNodeIDs; }
    inline common::JoinType getJoinType() const { return joinType; }

    inline std::shared_ptr<binder::Expression> getMark() const {
        assert(joinType == common::JoinType::MARK && mark);
        return mark;
    }
    inline bool getIsProbeAcc() const { return isProbeAcc; }
    inline Schema* getBuildSideSchema() const { return children[1]->getSchema(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinNodeIDs, joinType, mark, markPos, isProbeAcc,
            expressionsToMaterialize, children[0]->copy(), children[1]->copy());
    }

private:
    binder::expression_vector joinNodeIDs;
    common::JoinType joinType;
    std::shared_ptr<binder::Expression> mark; // when joinType is Mark
    uint32_t markPos;
    bool isProbeAcc;
    binder::expression_vector expressionsToMaterialize;
};

} // namespace planner
} // namespace kuzu
