#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalLimit : public LogicalOperator {

public:
    LogicalLimit(uint64_t limitNumber, uint32_t groupPosToSelect,
        unordered_set<uint32_t> groupsPosInScope, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, limitNumber{limitNumber},
          groupPosToSelect{groupPosToSelect}, groupsPosInScope{move(groupsPosInScope)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_LIMIT; }

    string getExpressionsForPrinting() const override { return to_string(limitNumber); }

    inline uint64_t getLimitNumber() const { return limitNumber; }

    inline uint32_t getGroupPosToSelect() const { return groupPosToSelect; }

    inline unordered_set<uint32_t> getGroupsPosToLimit() const { return groupsPosInScope; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLimit>(
            limitNumber, groupPosToSelect, groupsPosInScope, children[0]->copy());
    }

private:
    uint64_t limitNumber;
    uint32_t groupPosToSelect;
    unordered_set<uint32_t> groupsPosInScope;
};

} // namespace planner
} // namespace kuzu
