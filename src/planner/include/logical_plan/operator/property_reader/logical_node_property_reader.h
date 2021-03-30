#pragma once

#include <string>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace std;

namespace graphflow {
namespace planner {

class LogicalNodePropertyReader : public LogicalOperator {

public:
    LogicalNodePropertyReader(const string& nodeVarName, const string& nodeLabel,
        const string& propertyName, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, nodeVarName{nodeVarName}, nodeLabel{nodeLabel},
          propertyName{propertyName} {}

    LogicalOperatorType getLogicalOperatorType() const {
        return LogicalOperatorType::LOGICAL_NODE_PROPERTY_READER;
    }

public:
    const string nodeVarName;
    const string nodeLabel;
    const string propertyName;
};

} // namespace planner
} // namespace graphflow
