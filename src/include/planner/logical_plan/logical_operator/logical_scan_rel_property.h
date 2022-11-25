#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalScanRelProperty : public LogicalOperator {
public:
    LogicalScanRelProperty(shared_ptr<NodeExpression> boundNodeExpression,
        shared_ptr<NodeExpression> nbrNodeExpression, table_id_t relTableID, RelDirection direction,
        string propertyName, uint32_t propertyID, bool isColumn, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, boundNodeExpression{move(boundNodeExpression)},
          nbrNodeExpression{move(nbrNodeExpression)}, relTableID{relTableID}, direction{direction},
          propertyName{move(propertyName)}, propertyID{propertyID}, isColumn{isColumn} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    string getExpressionsForPrinting() const override { return propertyName; }

    inline shared_ptr<NodeExpression> getBoundNodeExpression() const { return boundNodeExpression; }
    inline shared_ptr<NodeExpression> getNbrNodeExpression() const { return nbrNodeExpression; }
    inline table_id_t getRelTableID() const { return relTableID; }
    inline RelDirection getDirection() const { return direction; }
    inline string getPropertyName() const { return propertyName; }
    inline uint32_t getPropertyID() const { return propertyID; }
    inline bool getIsColumn() const { return isColumn; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanRelProperty>(boundNodeExpression, nbrNodeExpression,
            relTableID, direction, propertyName, propertyID, isColumn, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> boundNodeExpression;
    shared_ptr<NodeExpression> nbrNodeExpression;
    table_id_t relTableID;
    RelDirection direction;
    string propertyName;
    uint32_t propertyID;
    bool isColumn;
};

} // namespace planner
} // namespace kuzu
