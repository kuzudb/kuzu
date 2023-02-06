#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalSkip : public LogicalOperator {
public:
    LogicalSkip(
        uint64_t skipNumber, uint32_t groupPosToSelect, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::SKIP, std::move(child)), skipNumber{skipNumber},
          groupPosToSelect{groupPosToSelect} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        return std::to_string(skipNumber);
    }

    inline uint64_t getSkipNumber() const { return skipNumber; }

    inline uint32_t getGroupPosToSelect() const { return groupPosToSelect; }

    inline std::unordered_set<uint32_t> getGroupsPosToSkip() const {
        return schema->getGroupsPosInScope();
    };

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSkip>(skipNumber, groupPosToSelect, children[0]->copy());
    }

private:
    uint64_t skipNumber;
    uint32_t groupPosToSelect;
};

} // namespace planner
} // namespace kuzu
