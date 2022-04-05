#pragma once

#include "base_logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanRelProperty : public LogicalOperator {

public:
    LogicalScanRelProperty(string boundNodeID, label_t boundNodeLabel, string nbrNodeID,
        label_t relLabel, RelDirection direction, string propertyName, uint32_t propertyKey,
        bool isColumn, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, boundNodeID{move(boundNodeID)},
          boundNodeLabel{boundNodeLabel}, nbrNodeID{move(nbrNodeID)}, relLabel{relLabel},
          direction{direction}, propertyName{move(propertyName)},
          propertyKey{propertyKey}, isColumn{isColumn} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    string getExpressionsForPrinting() const override { return propertyName; }

    inline string getBoundNodeID() const { return boundNodeID; }

    inline label_t getBoundNodeLabel() const { return boundNodeLabel; }

    inline string getNbrNodeID() const { return nbrNodeID; }

    inline label_t getRelLabel() const { return relLabel; }

    inline RelDirection getDirection() const { return direction; }

    inline string getPropertyName() const { return propertyName; }

    inline uint32_t getPropertyKey() const { return propertyKey; }

    inline bool getIsColumn() const { return isColumn; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanRelProperty>(boundNodeID, boundNodeLabel, nbrNodeID, relLabel,
            direction, propertyName, propertyKey, isColumn, children[0]->copy());
    }

private:
    string boundNodeID;
    label_t boundNodeLabel;
    string nbrNodeID;
    label_t relLabel;
    RelDirection direction;
    string propertyName;
    uint32_t propertyKey;
    bool isColumn;
};

} // namespace planner
} // namespace graphflow
