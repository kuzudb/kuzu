#pragma once

#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_property.h"

namespace graphflow {
namespace planner {

class LogicalScanRelProperty : public LogicalScanProperty {

public:
    LogicalScanRelProperty(string boundNodeID, label_t boundNodeLabel, string nbrNodeID,
        label_t relLabel, Direction direction, string propertyName, uint32_t propertyKey,
        bool isColumn, shared_ptr<LogicalOperator> child)
        : LogicalScanRelProperty{move(boundNodeID), boundNodeLabel, move(nbrNodeID), relLabel,
              direction, vector<string>{propertyName}, vector<uint32_t>{propertyKey}, isColumn,
              move(child)} {}

    LogicalScanRelProperty(string boundNodeID, label_t boundNodeLabel, string nbrNodeID,
        label_t relLabel, Direction direction, vector<string> propertyNames,
        vector<uint32_t> propertyKeys, bool isColumn, shared_ptr<LogicalOperator> child)
        : LogicalScanProperty{move(propertyNames), move(propertyKeys), move(child)},
          boundNodeID{move(boundNodeID)}, boundNodeLabel{boundNodeLabel},
          nbrNodeID{move(nbrNodeID)}, relLabel{relLabel}, direction{direction}, isColumn{isColumn} {
    }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    inline string getBoundNodeID() const { return boundNodeID; }

    inline label_t getBoundNodeLabel() const { return boundNodeLabel; }

    inline string getNbrNodeID() const { return nbrNodeID; }

    inline label_t getRelLabel() const { return relLabel; }

    inline Direction getDirection() const { return direction; }

    inline bool getIsColumn() const { return isColumn; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanRelProperty>(boundNodeID, boundNodeLabel, nbrNodeID, relLabel,
            direction, propertyNames, propertyKeys, isColumn, children[0]->copy());
    }

private:
    string boundNodeID;
    label_t boundNodeLabel;
    string nbrNodeID;
    label_t relLabel;
    Direction direction;
    bool isColumn;
};

} // namespace planner
} // namespace graphflow
