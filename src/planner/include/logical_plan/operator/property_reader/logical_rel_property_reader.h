#pragma once

#include <string>

#include "src/common/include/types.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalRelPropertyReader : public LogicalOperator {

public:
    LogicalRelPropertyReader(const string& relName, const string& boundNodeVarName,
        const string& boundNodeVarLabel, const string& nbrNodeVarName,
        const string& nbrNodeVarLabel, const string& relLabel, Direction direction,
        const string& propertyName, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, relName{relName}, boundNodeVarName{boundNodeVarName},
          boundNodeVarLabel{boundNodeVarLabel}, nbrNodeVarName{nbrNodeVarName},
          nbrNodeVarLabel{nbrNodeVarLabel}, relLabel{relLabel}, direction{direction},
          propertyName{propertyName} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_REL_PROPERTY_READER;
    }

    string getOperatorInformation() const override { return relName + "." + propertyName; }

public:
    const string relName;
    const string boundNodeVarName;
    const string boundNodeVarLabel;
    const string nbrNodeVarName;
    const string nbrNodeVarLabel;
    const string relLabel;
    const Direction direction;
    const string propertyName;
};

} // namespace planner
} // namespace graphflow
