#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

struct BuildInfo {
    BuildInfo(
        shared_ptr<NodeExpression> key, unique_ptr<Schema> schema, expression_vector expressions)
        : key{move(key)}, schema{move(schema)}, expressionsToMaterialize{move(expressions)} {}

    inline unique_ptr<BuildInfo> copy() {
        return make_unique<BuildInfo>(key, schema->copy(), expressionsToMaterialize);
    }

    shared_ptr<NodeExpression> key;
    unique_ptr<Schema> schema;
    expression_vector expressionsToMaterialize;
};

class LogicalIntersect : public LogicalOperator {

public:
    LogicalIntersect(shared_ptr<NodeExpression> intersectNode, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, intersectNode{move(intersectNode)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_INTERSECT;
    }

    string getExpressionsForPrinting() const override { return intersectNode->getRawName(); }

    inline void addChild(shared_ptr<LogicalOperator> op, unique_ptr<BuildInfo> buildInfo) {
        children.push_back(move(op));
        buildInfos.push_back(move(buildInfo));
    }
    inline shared_ptr<NodeExpression> getIntersectNode() const { return intersectNode; }
    inline BuildInfo* getBuildInfo(uint32_t idx) const { return buildInfos[idx].get(); }

    unique_ptr<LogicalOperator> copy() override {
        auto result = make_unique<LogicalIntersect>(intersectNode, children[0]->copy());
        for (auto i = 1u; i < children.size(); ++i) {
            result->addChild(children[i]->copy(), buildInfos[i]->copy());
        }
        return result;
    }

private:
    shared_ptr<NodeExpression> intersectNode;
    vector<unique_ptr<BuildInfo>> buildInfos;
};

} // namespace planner
} // namespace graphflow