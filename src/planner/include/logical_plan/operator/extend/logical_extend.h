#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalExtend : public LogicalOperator {

public:
    LogicalExtend(const string& boundNodeID, label_t boundNodeLabel, const string& nbrNodeID,
        label_t nbrNodeLabel, label_t relLabel, Direction direction, bool isColumn,
        uint8_t lowerBound, uint8_t upperBound, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, boundNodeID{boundNodeID}, boundNodeLabel{boundNodeLabel},
          nbrNodeID{nbrNodeID}, nbrNodeLabel{nbrNodeLabel}, relLabel{relLabel},
          direction{direction}, isColumn{isColumn}, lowerBound{lowerBound}, upperBound{upperBound} {
    }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getExpressionsForPrinting() const override {
        return boundNodeID + (direction == Direction::FWD ? "->" : "<-") + nbrNodeID;
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
