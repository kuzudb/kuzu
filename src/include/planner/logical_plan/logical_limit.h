#pragma once

#include "logical_operator.h"
#include "planner/logical_plan/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

class LogicalLimit : public LogicalOperator {
public:
    LogicalLimit(uint64_t limitNumber, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::LIMIT, std::move(child)}, limitNumber{limitNumber} {}

    f_group_pos_set getGroupsPosToFlatten();

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

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

} // namespace planner
} // namespace kuzu
