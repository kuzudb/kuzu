#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalLimit : public LogicalOperator {

public:
    LogicalLimit(uint64_t limitNumber, uint32_t groupPosToSelect, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::LIMIT, std::move(child)}, limitNumber{limitNumber},
          groupPosToSelect{groupPosToSelect} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline string getExpressionsForPrinting() const override { return to_string(limitNumber); }

    inline uint64_t getLimitNumber() const { return limitNumber; }

    inline uint32_t getGroupPosToSelect() const { return groupPosToSelect; }

    inline unordered_set<uint32_t> getGroupsPosToLimit() const {
        return schema->getGroupsPosInScope();
    }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLimit>(limitNumber, groupPosToSelect, children[0]->copy());
    }

private:
    uint64_t limitNumber;
    uint32_t groupPosToSelect;
};

} // namespace planner
} // namespace kuzu
