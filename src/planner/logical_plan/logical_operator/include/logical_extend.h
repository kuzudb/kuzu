#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

class LogicalExtend : public LogicalOperator {
public:
    LogicalExtend(shared_ptr<NodeExpression> boundNodeExpression,
        shared_ptr<NodeExpression> nbrNodeExpression, label_t relLabel, RelDirection direction,
        bool isColumn, uint8_t lowerBound, uint8_t upperBound, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, boundNodeExpression{move(boundNodeExpression)},
          nbrNodeExpression{move(nbrNodeExpression)}, relLabel{relLabel}, direction{direction},
          isColumn{isColumn}, lowerBound{lowerBound}, upperBound{upperBound} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getExpressionsForPrinting() const override {
        return boundNodeExpression->getRawName() + (direction == RelDirection::FWD ? "->" : "<-") +
               nbrNodeExpression->getRawName();
    }

    inline shared_ptr<NodeExpression> getBoundNodeExpression() const { return boundNodeExpression; }
    inline shared_ptr<NodeExpression> getNbrNodeExpression() const { return nbrNodeExpression; }
    inline label_t getRelLabel() const { return relLabel; }
    inline RelDirection getDirection() const { return direction; }
    inline bool getIsColumn() const { return isColumn; }
    inline uint8_t getLowerBound() const { return lowerBound; }
    inline uint8_t getUpperBound() const { return upperBound; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(boundNodeExpression, nbrNodeExpression, relLabel,
            direction, isColumn, lowerBound, upperBound, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> boundNodeExpression;
    shared_ptr<NodeExpression> nbrNodeExpression;
    label_t relLabel;
    RelDirection direction;
    bool isColumn;
    uint8_t lowerBound;
    uint8_t upperBound;
};

} // namespace planner
} // namespace graphflow
