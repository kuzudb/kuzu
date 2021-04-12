#pragma once

#include <string>

#include "src/common/include/types.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/query_graph/query_rel.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalExtend : public LogicalOperator {

public:
    LogicalExtend(const QueryRel& queryRel, const Direction& direction,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, direction{direction} {
        auto isFwd = FWD == direction;
        boundNodeVarName = isFwd ? queryRel.getSrcNodeName() : queryRel.getDstNodeName();
        boundNodeVarLabel = isFwd ? queryRel.srcNode->label : queryRel.dstNode->label;
        nbrNodeVarName = isFwd ? queryRel.getDstNodeName() : queryRel.getSrcNodeName();
        nbrNodeVarLabel = isFwd ? queryRel.dstNode->label : queryRel.srcNode->label;
        relLabel = queryRel.label;
    }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getOperatorInformation() const override {
        return boundNodeVarName + (direction == Direction::FWD ? "->" : "<-") + nbrNodeVarName;
    }

public:
    string boundNodeVarName;
    label_t boundNodeVarLabel;
    string nbrNodeVarName;
    label_t nbrNodeVarLabel;
    label_t relLabel;
    Direction direction;
};

} // namespace planner
} // namespace graphflow
