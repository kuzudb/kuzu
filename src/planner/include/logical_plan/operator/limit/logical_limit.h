#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalLimit : public LogicalOperator {

public:
    LogicalLimit(uint64_t limitNumber, uint32_t groupPosToSelect, vector<uint32_t> groupsPosToLimit,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, limitNumber{limitNumber},
          groupPosToSelect{groupPosToSelect}, groupsPosToLimit{move(groupsPosToLimit)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_LIMIT; }

    string getExpressionsForPrinting() const override { return to_string(limitNumber); }

    inline uint64_t getLimitNumber() const { return limitNumber; }
    inline uint32_t getGroupPosToSelect() const { return groupPosToSelect; }
    inline const vector<uint32_t>& getGroupsPosToLimit() const { return groupsPosToLimit; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLimit>(
            limitNumber, groupPosToSelect, groupsPosToLimit, children[0]->copy());
    }

private:
    uint64_t limitNumber;
    uint32_t groupPosToSelect;
    vector<uint32_t> groupsPosToLimit;
};

} // namespace planner
} // namespace graphflow
