#pragma once

#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_property.h"

namespace graphflow {
namespace planner {

class LogicalScanRelProperty : public LogicalScanProperty {

public:
    LogicalScanRelProperty(string boundNodeID, label_t boundNodeLabel, string nbrNodeID,
        label_t relLabel, Direction direction, string propertyExpressionName, uint32_t propertyKey,
        bool isColumn, shared_ptr<LogicalOperator> child)
        : LogicalScanProperty{move(propertyExpressionName), propertyKey, move(child)},
          boundNodeID{move(boundNodeID)}, boundNodeLabel{boundNodeLabel},
          nbrNodeID{move(nbrNodeID)}, relLabel{relLabel}, direction{direction}, isColumn{isColumn} {
    }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanRelProperty>(boundNodeID, boundNodeLabel, nbrNodeID, relLabel,
            direction, propertyExpressionName, propertyKey, isColumn, children[0]->copy());
    }

public:
    string boundNodeID;
    label_t boundNodeLabel;
    string nbrNodeID;
    label_t relLabel;
    Direction direction;
    bool isColumn;
};

} // namespace planner
} // namespace graphflow
