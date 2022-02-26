#pragma once

#include "base_logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalExtend : public LogicalOperator {

public:
    LogicalExtend(string boundNodeID, label_t boundNodeLabel, string nbrNodeID,
        label_t nbrNodeLabel, label_t relLabel, Direction direction, bool isColumn,
        uint8_t lowerBound, uint8_t upperBound, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, boundNodeID{move(boundNodeID)},
          boundNodeLabel{boundNodeLabel}, nbrNodeID{move(nbrNodeID)},
          nbrNodeLabel{nbrNodeLabel}, relLabel{relLabel}, direction{direction}, isColumn{isColumn},
          lowerBound{lowerBound}, upperBound{upperBound} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getExpressionsForPrinting() const override {
        return boundNodeID + (direction == Direction::FWD ? "->" : "<-") + nbrNodeID;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(boundNodeID, boundNodeLabel, nbrNodeID, nbrNodeLabel,
            relLabel, direction, isColumn, lowerBound, upperBound, children[0]->copy());
    }

public:
    string boundNodeID;
    label_t boundNodeLabel;
    string nbrNodeID;
    label_t nbrNodeLabel;
    label_t relLabel;
    Direction direction;
    bool isColumn;
    uint8_t lowerBound;
    uint8_t upperBound;
};

} // namespace planner
} // namespace graphflow
