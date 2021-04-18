#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalExtend : public LogicalOperator {

public:
    LogicalExtend(string boundNodeID, label_t boundNodeLabel, string nbrNodeID,
        label_t nbrNodeLabel, label_t relLabel, Direction direction,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, boundNodeID{move(boundNodeID)},
          boundNodeLabel{boundNodeLabel}, nbrNodeID{move(nbrNodeID)},
          nbrNodeLabel{nbrNodeLabel}, relLabel{relLabel}, direction{direction} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getOperatorInformation() const override {
        return boundNodeID + (direction == Direction::FWD ? "->" : "<-") + nbrNodeID;
    }

public:
    string boundNodeID;
    label_t boundNodeLabel;
    string nbrNodeID;
    label_t nbrNodeLabel;
    label_t relLabel;
    Direction direction;
};

} // namespace planner
} // namespace graphflow
