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
    LogicalExtend(const string& boundNodeVarName, const string& boundNodeVarLabel,
        const string& nbrNodeVarName, const string& nbrNodeVarLabel, const string& relLabel,
        const Direction& direction, unique_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, boundNodeVarName{boundNodeVarName},
          boundNodeVarLabel{boundNodeVarLabel}, nbrNodeVarName{nbrNodeVarName},
          nbrNodeVarLabel{nbrNodeVarLabel}, relLabel{relLabel}, direction{direction} {}

    LogicalOperatorType getLogicalOperatorType() const { return LogicalOperatorType::EXTEND; }

public:
    const string boundNodeVarName;
    const string boundNodeVarLabel;
    const string nbrNodeVarName;
    const string nbrNodeVarLabel;
    const string relLabel;
    const Direction direction;
};

} // namespace planner
} // namespace graphflow
