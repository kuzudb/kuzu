#pragma once

#include "base_logical_operator.h"
#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

class LogicalSkip : public LogicalOperator {
public:
    LogicalSkip(uint64_t skipNumber, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::SKIP, std::move(child)), skipNumber{skipNumber} {}

    inline void computeSchema() override { copyChildSchema(0); }

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

class LogicalSkipFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(LogicalSkip* skip) {
        return getGroupsPosToFlatten(skip->getChild(0).get());
    }
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(LogicalOperator* skipChild) {
        auto dependentGroupsPos = skipChild->getSchema()->getGroupsPosInScope();
        return FlattenAllButOneFactorizationSolver::getGroupsPosToFlatten(
            dependentGroupsPos, skipChild->getSchema());
    }
};

} // namespace planner
} // namespace kuzu
