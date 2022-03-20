#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/node_expression.h"
#include "src/binder/expression/include/rel_expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalExtend : public LogicalOperator {

public:
    LogicalExtend(
        shared_ptr<RelExpression> queryRel, Direction direction, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, queryRel{queryRel}, direction{move(direction)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getExpressionsForPrinting() const override {
        return getBoundNodeID() + (direction == Direction::FWD ? "->" : "<-") + getNbrNodeID();
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(queryRel, direction, children[0]->copy());
    }

    inline shared_ptr<NodeExpression> getBoundNode() const {
        return FWD == direction ? queryRel->getSrcNode() : queryRel->getDstNode();
    }

    inline shared_ptr<NodeExpression> getNbrNode() const {
        return FWD == direction ? queryRel->getDstNode() : queryRel->getSrcNode();
    }

    inline string getBoundNodeID() const { return getBoundNode()->getIDProperty(); }

    inline label_t getBoundNodeLabel() const { return getBoundNode()->getLabel(); }

    inline string getNbrNodeID() const { return getNbrNode()->getIDProperty(); }

    inline label_t getNbrNodeLabel() const { return getNbrNode()->getLabel(); }

    inline label_t getRelLabel() const { return queryRel->getLabel(); }

    inline uint8_t getUpperBound() const { return queryRel->getUpperBound(); }

    inline uint8_t getLowerBound() const { return queryRel->getLowerBound(); }

    inline Direction getDirection() const { return direction; }

protected:
    shared_ptr<RelExpression> queryRel;
    Direction direction;
};

} // namespace planner
} // namespace graphflow
