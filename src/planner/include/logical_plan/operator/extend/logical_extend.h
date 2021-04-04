#pragma once

#include <string>

#include "src/common/include/types.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalExtend : public LogicalOperator {

public:
    LogicalExtend(const string& boundNodeVarName, label_t boundNodeVarLabel,
        const string& nbrNodeVarName, label_t nbrNodeVarLabel, label_t relLabel,
        const Direction& direction, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, boundNodeVarName{boundNodeVarName},
          boundNodeVarLabel{boundNodeVarLabel}, nbrNodeVarName{nbrNodeVarName},
          nbrNodeVarLabel{nbrNodeVarLabel}, relLabel{relLabel}, direction{direction} {}

    LogicalOperatorType getLogicalOperatorType() const {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

public:
    const string boundNodeVarName;
    const label_t boundNodeVarLabel;
    const string nbrNodeVarName;
    const label_t nbrNodeVarLabel;
    const label_t relLabel;
    const Direction direction;
};

} // namespace planner
} // namespace graphflow
