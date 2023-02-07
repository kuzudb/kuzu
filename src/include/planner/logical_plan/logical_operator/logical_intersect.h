#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "schema.h"

namespace kuzu {
namespace planner {

struct LogicalIntersectBuildInfo {
    LogicalIntersectBuildInfo(
        std::shared_ptr<binder::Expression> keyNodeID, binder::expression_vector expressions)
        : keyNodeID{std::move(keyNodeID)}, expressionsToMaterialize{std::move(expressions)} {}

    inline std::unique_ptr<LogicalIntersectBuildInfo> copy() {
        return make_unique<LogicalIntersectBuildInfo>(keyNodeID, expressionsToMaterialize);
    }

    std::shared_ptr<binder::Expression> keyNodeID;
    binder::expression_vector expressionsToMaterialize;
};

class LogicalIntersect : public LogicalOperator {
public:
    LogicalIntersect(std::shared_ptr<binder::Expression> intersectNodeID,
        std::shared_ptr<LogicalOperator> probeChild,
        std::vector<std::shared_ptr<LogicalOperator>> buildChildren,
        std::vector<std::unique_ptr<LogicalIntersectBuildInfo>> buildInfos)
        : LogicalOperator{LogicalOperatorType::INTERSECT, std::move(probeChild)},
          intersectNodeID{std::move(intersectNodeID)}, buildInfos{std::move(buildInfos)} {
        for (auto& child : buildChildren) {
            children.push_back(std::move(child));
        }
    }

    void computeSchema() override;

    std::string getExpressionsForPrinting() const override { return intersectNodeID->getRawName(); }

    inline std::shared_ptr<binder::Expression> getIntersectNodeID() const {
        return intersectNodeID;
    }
    inline LogicalIntersectBuildInfo* getBuildInfo(uint32_t idx) const {
        return buildInfos[idx].get();
    }
    inline uint32_t getNumBuilds() const { return buildInfos.size(); }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    std::shared_ptr<binder::Expression> intersectNodeID;
    std::vector<std::unique_ptr<LogicalIntersectBuildInfo>> buildInfos;
};

} // namespace planner
} // namespace kuzu
