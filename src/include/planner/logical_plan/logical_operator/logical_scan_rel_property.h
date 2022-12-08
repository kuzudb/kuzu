#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {

class LogicalScanRelProperty : public LogicalOperator {
public:
    LogicalScanRelProperty(shared_ptr<NodeExpression> boundNode, shared_ptr<NodeExpression> nbrNode,
        shared_ptr<RelExpression> rel, RelDirection direction, shared_ptr<Expression> property,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, boundNode{std::move(boundNode)}, nbrNode{std::move(
                                                                                  nbrNode)},
          rel{std::move(rel)}, direction{direction}, property{std::move(property)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    inline string getExpressionsForPrinting() const override { return property->getRawName(); }

    inline void computeSchema(Schema& schema) {
        auto nbrGroupPos = schema.getGroupPos(nbrNode->getInternalIDPropertyName());
        schema.insertToGroupAndScope(property, nbrGroupPos);
    }

    inline shared_ptr<NodeExpression> getBoundNode() const { return boundNode; }
    inline shared_ptr<NodeExpression> getNbrNode() const { return nbrNode; }
    inline shared_ptr<RelExpression> getRel() const { return rel; }
    inline RelDirection getDirection() const { return direction; }
    inline shared_ptr<Expression> getProperty() const { return property; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanRelProperty>(
            boundNode, nbrNode, rel, direction, property, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> boundNode;
    shared_ptr<NodeExpression> nbrNode;
    shared_ptr<RelExpression> rel;
    RelDirection direction;
    shared_ptr<Expression> property;
};

} // namespace planner
} // namespace kuzu
