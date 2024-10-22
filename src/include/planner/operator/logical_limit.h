#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalLimit final : public LogicalOperator {
public:
    LogicalLimit(uint64_t skipNum, uint64_t limitNum, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::LIMIT, std::move(child)}, skipNum{skipNum},
          limitNum{limitNum} {}

    f_group_pos_set getGroupsPosToFlatten();

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override;

    inline bool hasSkipNum() const { return skipNum != UINT64_MAX; }
    inline uint64_t getSkipNum() const { return skipNum; }
    inline bool hasLimitNum() const { return limitNum != UINT64_MAX; }
    inline uint64_t getLimitNum() const { return limitNum; }

    f_group_pos getGroupPosToSelect() const;

    inline std::unordered_set<f_group_pos> getGroupsPosToLimit() const {
        return schema->getGroupsPosInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLimit>(skipNum, limitNum, children[0]->copy());
    }

private:
    uint64_t skipNum;
    uint64_t limitNum;
};

} // namespace planner
} // namespace kuzu
