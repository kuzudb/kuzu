#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalSkip : public LogicalOperator {

public:
    LogicalSkip(uint64_t skipNumber, uint32_t groupPosToSelect, vector<uint32_t> groupsPosToSkip,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator(move(prevOperator)), skipNumber{skipNumber},
          groupPosToSelect{groupPosToSelect}, groupsPosToSkip{move(groupsPosToSkip)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_SKIP; }

    string getExpressionsForPrinting() const override { return to_string(skipNumber); }

    inline uint64_t getSkipNumber() const { return skipNumber; }
    inline uint32_t getGroupPosToSelect() const { return groupPosToSelect; }
    inline const vector<uint32_t>& getGroupsPosToSkip() const { return groupsPosToSkip; };

private:
    uint64_t skipNumber;
    uint32_t groupPosToSelect;
    vector<uint32_t> groupsPosToSkip;
};

} // namespace planner
} // namespace graphflow
