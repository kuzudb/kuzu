#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanRelProperty : public LogicalOperator {

public:
    LogicalScanRelProperty(string boundNodeID, label_t boundNodeLabel, string nbrNodeID,
        label_t nbrNodeLabel, string relName, label_t relLabel, Direction direction,
        string propertyName, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, boundNodeID{move(boundNodeID)},
          boundNodeLabel{boundNodeLabel}, nbrNodeID{move(nbrNodeID)}, nbrNodeLabel{nbrNodeLabel},
          relName{move(relName)}, relLabel{relLabel}, direction{direction},
          propertyName(move(propertyName)) {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    string getOperatorInformation() const override { return relName + "." + propertyName; }

public:
    string boundNodeID;
    label_t boundNodeLabel;
    string nbrNodeID;
    label_t nbrNodeLabel;
    string relName;
    label_t relLabel;
    Direction direction;
    string propertyName;
};

} // namespace planner
} // namespace graphflow
