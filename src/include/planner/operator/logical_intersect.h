#pragma once

#include "planner/operator/logical_operator.h"
#include "planner/operator/sip/side_way_info_passing.h"

namespace kuzu {
namespace planner {

class LogicalIntersect final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::INTERSECT;

public:
    LogicalIntersect(std::shared_ptr<binder::Expression> intersectNodeID,
        binder::expression_vector keyNodeIDs, std::shared_ptr<LogicalOperator> probeChild,
        std::vector<std::shared_ptr<LogicalOperator>> buildChildren)
        : LogicalOperator{type_, std::move(probeChild)},
          intersectNodeID{std::move(intersectNodeID)}, keyNodeIDs{std::move(keyNodeIDs)} {
        for (auto& child : buildChildren) {
            children.push_back(std::move(child));
        }
    }

    f_group_pos_set getGroupsPosToFlattenOnProbeSide();
    f_group_pos_set getGroupsPosToFlattenOnBuildSide(uint32_t buildIdx);

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return intersectNodeID->toString(); }

    std::shared_ptr<binder::Expression> getIntersectNodeID() const { return intersectNodeID; }
    uint32_t getNumBuilds() const { return keyNodeIDs.size(); }
    binder::expression_vector getKeyNodeIDs() const { return keyNodeIDs; }
    std::shared_ptr<binder::Expression> getKeyNodeID(uint32_t idx) const { return keyNodeIDs[idx]; }

    SIPInfo& getSIPInfoUnsafe() { return sipInfo; }
    SIPInfo getSIPInfo() const { return sipInfo; }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    std::shared_ptr<binder::Expression> intersectNodeID;
    binder::expression_vector keyNodeIDs;
    SIPInfo sipInfo;
};

} // namespace planner
} // namespace kuzu
