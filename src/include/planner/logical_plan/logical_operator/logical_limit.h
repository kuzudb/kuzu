#pragma once

#include "base_logical_operator.h"
#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

class LogicalLimit : public LogicalOperator {
public:
    LogicalLimit(uint64_t limitNumber, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::LIMIT, std::move(child)}, limitNumber{limitNumber} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        return std::to_string(limitNumber);
    }

    inline uint64_t getLimitNumber() const { return limitNumber; }

    f_group_pos getGroupPosToSelect() const;

    inline std::unordered_set<f_group_pos> getGroupsPosToLimit() const {
        return schema->getGroupsPosInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLimit>(limitNumber, children[0]->copy());
    }

private:
    uint64_t limitNumber;
};

class LogicalLimitFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(LogicalLimit* limit) {
        return getGroupsPosToFlatten(limit->getChild(0).get());
    }
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(LogicalOperator* limitChild) {
        auto dependentGroupsPos = limitChild->getSchema()->getGroupsPosInScope();
        return FlattenAllButOneFactorizationSolver::getGroupsPosToFlatten(
            dependentGroupsPos, limitChild->getSchema());
    }
};

} // namespace planner
} // namespace kuzu
