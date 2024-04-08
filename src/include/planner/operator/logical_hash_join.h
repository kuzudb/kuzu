#pragma once

#include "binder/expression/expression_util.h"
#include "common/enums/join_type.h"
#include "logical_operator.h"
#include "planner/operator/sip/side_way_info_passing.h"

namespace kuzu {
namespace planner {

// We only support equality comparison as join condition
using join_condition_t = binder::expression_pair;

enum class JoinSubPlanSolveOrder : uint8_t {
    ANY = 0,
    PROBE_BUILD = 1,
    BUILD_PROBE = 2,
};

// Probe side on left, i.e. children[0]. Build side on right, i.e. children[1].
class LogicalHashJoin : public LogicalOperator {
public:
    LogicalHashJoin(std::vector<join_condition_t> joinConditions, common::JoinType joinType,
        std::shared_ptr<binder::Expression> mark, std::shared_ptr<LogicalOperator> probeSideChild,
        std::shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{LogicalOperatorType::HASH_JOIN, std::move(probeSideChild),
              std::move(buildSideChild)},
          joinConditions(std::move(joinConditions)), joinType{joinType}, mark{std::move(mark)},
          sip{SidewaysInfoPassing::NONE}, order{JoinSubPlanSolveOrder::ANY} {}

    f_group_pos_set getGroupsPosToFlattenOnProbeSide();
    f_group_pos_set getGroupsPosToFlattenOnBuildSide();

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return isNodeIDOnlyJoin() ? binder::ExpressionUtil::toString(getJoinNodeIDs()) :
                                    binder::ExpressionUtil::toString(joinConditions);
    }

    binder::expression_vector getExpressionsToMaterialize() const;

    binder::expression_vector getJoinNodeIDs() const;

    inline std::vector<join_condition_t> getJoinConditions() const { return joinConditions; }
    inline common::JoinType getJoinType() const { return joinType; }
    bool hasMark() const { return mark != nullptr; }
    inline std::shared_ptr<binder::Expression> getMark() const { return mark; }
    inline void setSIP(SidewaysInfoPassing sip_) { sip = sip_; }
    inline SidewaysInfoPassing getSIP() const { return sip; }

    inline void setJoinSubPlanSolveOrder(JoinSubPlanSolveOrder order_) { order = order_; }
    inline JoinSubPlanSolveOrder getJoinSubPlanSolveOrder() const { return order; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalHashJoin>(joinConditions, joinType, mark, children[0]->copy(),
            children[1]->copy());
    }

    // Flat probe side key group in either of the following two cases:
    // 1. there are multiple join nodes;
    // 2. if the build side contains more than one group or the build side has projected out data
    // chunks, which may increase the multiplicity of data chunks in the build side. The key is to
    // keep probe side key unflat only when we know that there is only 0 or 1 match for each key.
    // TODO(Guodong): when the build side has only flat payloads, we should consider getting rid of
    // flattening probe key, instead duplicating keys as in vectorized processing if necessary.
    bool requireFlatProbeKeys();

private:
    bool isNodeIDOnlyJoin() const;
    bool isJoinKeyUniqueOnBuildSide(const binder::Expression& joinNodeID);

private:
    std::vector<join_condition_t> joinConditions;
    common::JoinType joinType;
    std::shared_ptr<binder::Expression> mark; // when joinType is Mark or Left
    SidewaysInfoPassing sip;
    JoinSubPlanSolveOrder order; // sip introduce join dependency
};

} // namespace planner
} // namespace kuzu
