#pragma once

#include <string>

#include "src/common/include/types.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/query_graph/query_rel.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalScanRelProperty : public LogicalOperator {

public:
    LogicalScanRelProperty(const QueryRel& queryRel, Direction direction,
        const string& propertyName, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, direction{direction}, propertyName{propertyName} {
        auto isFwd = FWD == direction;
        boundNodeVarName = isFwd ? queryRel.getSrcNodeName() : queryRel.getDstNodeName();
        boundNodeVarLabel = isFwd ? queryRel.srcNode->label : queryRel.dstNode->label;
        nbrNodeVarName = isFwd ? queryRel.getDstNodeName() : queryRel.getSrcNodeName();
        nbrNodeVarLabel = isFwd ? queryRel.dstNode->label : queryRel.srcNode->label;
        relName = queryRel.name;
        relLabel = queryRel.label;
    }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_REL_PROPERTY;
    }

    string getOperatorInformation() const override { return relName + "." + propertyName; }

public:
    string boundNodeVarName;
    label_t boundNodeVarLabel;
    string nbrNodeVarName;
    label_t nbrNodeVarLabel;
    string relName;
    label_t relLabel;
    Direction direction;
    string propertyName;
};

} // namespace planner
} // namespace graphflow
