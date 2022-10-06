#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {

struct LogicalIntersectBuildInfo {
    shared_ptr<NodeExpression> key;
    unique_ptr<Schema> schema;
    expression_vector expressionsToMaterialize;

    LogicalIntersectBuildInfo(
        shared_ptr<NodeExpression> key, unique_ptr<Schema> schema, expression_vector expressions)
        : key{std::move(key)}, schema{std::move(schema)}, expressionsToMaterialize{
                                                              std::move(expressions)} {}

    inline unique_ptr<LogicalIntersectBuildInfo> copy() {
        return make_unique<LogicalIntersectBuildInfo>(
            key, schema->copy(), expressionsToMaterialize);
    }
};

class LogicalIntersect : public LogicalOperator {
public:
    LogicalIntersect(shared_ptr<NodeExpression> intersectNode, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, intersectNode{move(intersectNode)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_INTERSECT;
    }

    string getExpressionsForPrinting() const override { return intersectNode->getRawName(); }

    inline void addChild(
        shared_ptr<LogicalOperator> op, unique_ptr<LogicalIntersectBuildInfo> buildInfo) {
        children.push_back(std::move(op));
        buildInfos.push_back(std::move(buildInfo));
    }
    inline shared_ptr<NodeExpression> getIntersectNode() const { return intersectNode; }
    inline LogicalIntersectBuildInfo* getBuildInfo(uint32_t idx) const {
        return buildInfos[idx].get();
    }

    unique_ptr<LogicalOperator> copy() override {
        auto result = make_unique<LogicalIntersect>(intersectNode, children[0]->copy());
        for (auto i = 1u; i < children.size(); ++i) {
            result->addChild(children[i]->copy(), buildInfos[i]->copy());
        }
        return result;
    }

private:
    shared_ptr<NodeExpression> intersectNode;
    vector<unique_ptr<LogicalIntersectBuildInfo>> buildInfos;
};

} // namespace planner
} // namespace graphflow
