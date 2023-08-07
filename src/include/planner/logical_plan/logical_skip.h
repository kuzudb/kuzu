#pragma once

#include "logical_operator.h"
#include "planner/logical_plan/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

class LogicalSkip : public LogicalOperator {
public:
    LogicalSkip(uint64_t skipNumber, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::SKIP, std::move(child)), skipNumber{skipNumber} {}

    f_group_pos_set getGroupsPosToFlatten();

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        return std::to_string(skipNumber);
    }

    inline uint64_t getSkipNumber() const { return skipNumber; }

    f_group_pos getGroupPosToSelect() const;

    inline std::unordered_set<uint32_t> getGroupsPosToSkip() const {
        return schema->getGroupsPosInScope();
    };

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSkip>(skipNumber, children[0]->copy());
    }

private:
    uint64_t skipNumber;
};

} // namespace planner
} // namespace kuzu
