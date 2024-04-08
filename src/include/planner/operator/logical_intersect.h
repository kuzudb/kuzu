#pragma once

#include "planner/operator/logical_operator.h"
#include "planner/operator/sip/side_way_info_passing.h"

namespace kuzu {
namespace planner {

class LogicalIntersect : public LogicalOperator {
public:
    LogicalIntersect(std::shared_ptr<binder::Expression> intersectNodeID,
        binder::expression_vector keyNodeIDs, std::shared_ptr<LogicalOperator> probeChild,
        std::vector<std::shared_ptr<LogicalOperator>> buildChildren)
        : LogicalOperator{LogicalOperatorType::INTERSECT, std::move(probeChild)},
          intersectNodeID{std::move(intersectNodeID)}, keyNodeIDs{std::move(keyNodeIDs)},
          sip{SidewaysInfoPassing::NONE} {
        for (auto& child : buildChildren) {
            children.push_back(std::move(child));
        }
    }

    f_group_pos_set getGroupsPosToFlattenOnProbeSide();
    f_group_pos_set getGroupsPosToFlattenOnBuildSide(uint32_t buildIdx);

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return intersectNodeID->toString(); }

    inline std::shared_ptr<binder::Expression> getIntersectNodeID() const {
        return intersectNodeID;
    }
    inline uint32_t getNumBuilds() const { return keyNodeIDs.size(); }
    inline binder::expression_vector getKeyNodeIDs() const { return keyNodeIDs; }
    inline std::shared_ptr<binder::Expression> getKeyNodeID(uint32_t idx) const {
        return keyNodeIDs[idx];
    }
    inline void setSIP(SidewaysInfoPassing sip_) { sip = sip_; }
    inline SidewaysInfoPassing getSIP() const { return sip; }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    std::shared_ptr<binder::Expression> intersectNodeID;
    binder::expression_vector keyNodeIDs;
    SidewaysInfoPassing sip;
};

} // namespace planner
} // namespace kuzu
