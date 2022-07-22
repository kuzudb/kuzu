#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

class LogicalScanRelProperty : public LogicalOperator {
public:
    LogicalScanRelProperty(shared_ptr<NodeExpression> boundNodeExpression,
        shared_ptr<NodeExpression> nbrNodeExpression, label_t relLabel, RelDirection direction,
        string propertyName, uint32_t propertyKey, bool isColumn, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, boundNodeExpression{move(boundNodeExpression)},
          nbrNodeExpression{move(nbrNodeExpression)}, relLabel{relLabel}, direction{direction},
          propertyName{move(propertyName)}, propertyKey{propertyKey}, isColumn{isColumn} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    string getExpressionsForPrinting() const override { return propertyName; }

    inline shared_ptr<NodeExpression> getBoundNodeExpression() const { return boundNodeExpression; }
    inline shared_ptr<NodeExpression> getNbrNodeExpression() const { return nbrNodeExpression; }
    inline label_t getRelLabel() const { return relLabel; }
    inline RelDirection getDirection() const { return direction; }
    inline string getPropertyName() const { return propertyName; }
    inline uint32_t getPropertyKey() const { return propertyKey; }
    inline bool getIsColumn() const { return isColumn; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanRelProperty>(boundNodeExpression, nbrNodeExpression, relLabel,
            direction, propertyName, propertyKey, isColumn, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> boundNodeExpression;
    shared_ptr<NodeExpression> nbrNodeExpression;
    label_t relLabel;
    RelDirection direction;
    string propertyName;
    uint32_t propertyKey;
    bool isColumn;
};

} // namespace planner
} // namespace graphflow
