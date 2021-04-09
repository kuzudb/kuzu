#pragma once

#include <string>

#include "src/common/include/types.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/query_graph/query_rel.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalRelPropertyReader : public LogicalOperator {

public:
    LogicalRelPropertyReader(const QueryRel& queryRel, Direction direction,
        const string& propertyName, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, direction{direction}, propertyName{propertyName} {
        if (FWD == direction) {
            boundNodeVarName = queryRel.getSrcNodeName();
            boundNodeVarLabel = queryRel.srcNode->label;
            nbrNodeVarName = queryRel.getDstNodeName();
            nbrNodeVarLabel = queryRel.dstNode->label;
        } else {
            boundNodeVarName = queryRel.getDstNodeName();
            boundNodeVarLabel = queryRel.dstNode->label;
            nbrNodeVarName = queryRel.getSrcNodeName();
            nbrNodeVarLabel = queryRel.srcNode->label;
        }
        relName = queryRel.name;
        relLabel = queryRel.label;
    }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_REL_PROPERTY_READER;
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
