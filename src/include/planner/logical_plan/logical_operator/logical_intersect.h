#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "schema.h"

namespace kuzu {
namespace planner {

struct LogicalIntersectBuildInfo {
    LogicalIntersectBuildInfo(shared_ptr<NodeExpression> key, expression_vector expressions)
        : key{std::move(key)}, expressionsToMaterialize{std::move(expressions)} {}

    inline unique_ptr<LogicalIntersectBuildInfo> copy() {
        return make_unique<LogicalIntersectBuildInfo>(key, expressionsToMaterialize);
    }

    shared_ptr<NodeExpression> key;
    expression_vector expressionsToMaterialize;
};

class LogicalIntersect : public LogicalOperator {
public:
    LogicalIntersect(shared_ptr<NodeExpression> intersectNode,
        shared_ptr<LogicalOperator> probeChild, vector<shared_ptr<LogicalOperator>> buildChildren,
        vector<unique_ptr<LogicalIntersectBuildInfo>> buildInfos)
        : LogicalOperator{LogicalOperatorType::INTERSECT, std::move(probeChild)},
          intersectNode{std::move(intersectNode)}, buildInfos{std::move(buildInfos)} {
        for (auto& child : buildChildren) {
            children.push_back(std::move(child));
        }
    }

    void computeSchema() override;

    string getExpressionsForPrinting() const override { return intersectNode->getRawName(); }

    inline shared_ptr<NodeExpression> getIntersectNode() const { return intersectNode; }
    inline LogicalIntersectBuildInfo* getBuildInfo(uint32_t idx) const {
        return buildInfos[idx].get();
    }
    inline uint32_t getNumBuilds() const { return buildInfos.size(); }

    unique_ptr<LogicalOperator> copy() override;

private:
    shared_ptr<NodeExpression> intersectNode;
    vector<unique_ptr<LogicalIntersectBuildInfo>> buildInfos;
};

} // namespace planner
} // namespace kuzu
