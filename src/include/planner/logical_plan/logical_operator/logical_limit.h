#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalLimit : public LogicalOperator {

public:
    LogicalLimit(
        uint64_t limitNumber, uint32_t groupPosToSelect, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::LIMIT, std::move(child)}, limitNumber{limitNumber},
          groupPosToSelect{groupPosToSelect} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        return std::to_string(limitNumber);
    }

    inline uint64_t getLimitNumber() const { return limitNumber; }

    inline uint32_t getGroupPosToSelect() const { return groupPosToSelect; }

    inline std::unordered_set<uint32_t> getGroupsPosToLimit() const {
        return schema->getGroupsPosInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLimit>(limitNumber, groupPosToSelect, children[0]->copy());
    }

private:
    uint64_t limitNumber;
    uint32_t groupPosToSelect;
};

} // namespace planner
} // namespace kuzu
