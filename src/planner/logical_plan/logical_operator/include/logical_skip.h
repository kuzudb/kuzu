#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalSkip : public LogicalOperator {

public:
    LogicalSkip(uint64_t skipNumber, uint32_t groupPosToSelect,
        unordered_set<uint32_t> groupsPosInScope, shared_ptr<LogicalOperator> child)
        : LogicalOperator(move(child)), skipNumber{skipNumber}, groupPosToSelect{groupPosToSelect},
          groupsPosInScope{move(groupsPosInScope)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_SKIP; }

    string getExpressionsForPrinting() const override { return to_string(skipNumber); }

    inline uint64_t getSkipNumber() const { return skipNumber; }

    inline uint32_t getGroupPosToSelect() const { return groupPosToSelect; }

    inline unordered_set<uint32_t> getGroupsPosToSkip() const { return groupsPosInScope; };

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSkip>(
            skipNumber, groupPosToSelect, groupsPosInScope, children[0]->copy());
    }

private:
    uint64_t skipNumber;
    uint32_t groupPosToSelect;
    unordered_set<uint32_t> groupsPosInScope;
};

} // namespace planner
} // namespace kuzu
