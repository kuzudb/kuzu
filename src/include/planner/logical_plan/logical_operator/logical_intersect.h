#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "schema.h"

namespace kuzu {
namespace planner {

struct LogicalIntersectBuildInfo {
    LogicalIntersectBuildInfo(shared_ptr<Expression> keyNodeID, expression_vector expressions)
        : keyNodeID{std::move(keyNodeID)}, expressionsToMaterialize{std::move(expressions)} {}

    inline unique_ptr<LogicalIntersectBuildInfo> copy() {
        return make_unique<LogicalIntersectBuildInfo>(keyNodeID, expressionsToMaterialize);
    }

    shared_ptr<Expression> keyNodeID;
    expression_vector expressionsToMaterialize;
};

class LogicalIntersect : public LogicalOperator {
public:
    LogicalIntersect(shared_ptr<Expression> intersectNodeID, shared_ptr<LogicalOperator> probeChild,
        vector<shared_ptr<LogicalOperator>> buildChildren,
        vector<unique_ptr<LogicalIntersectBuildInfo>> buildInfos)
        : LogicalOperator{LogicalOperatorType::INTERSECT, std::move(probeChild)},
          intersectNodeID{std::move(intersectNodeID)}, buildInfos{std::move(buildInfos)} {
        for (auto& child : buildChildren) {
            children.push_back(std::move(child));
        }
    }

    void computeSchema() override;

    string getExpressionsForPrinting() const override { return intersectNodeID->getRawName(); }

    inline shared_ptr<Expression> getIntersectNodeID() const { return intersectNodeID; }
    inline LogicalIntersectBuildInfo* getBuildInfo(uint32_t idx) const {
        return buildInfos[idx].get();
    }
    inline uint32_t getNumBuilds() const { return buildInfos.size(); }

    unique_ptr<LogicalOperator> copy() override;

private:
    shared_ptr<Expression> intersectNodeID;
    vector<unique_ptr<LogicalIntersectBuildInfo>> buildInfos;
};

} // namespace planner
} // namespace kuzu
