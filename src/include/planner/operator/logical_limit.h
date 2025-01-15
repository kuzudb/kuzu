#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {
class LogicalLimit final : public LogicalOperator {
public:
    LogicalLimit(std::shared_ptr<binder::Expression> skipNum,
        std::shared_ptr<binder::Expression> limitNum, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::LIMIT, std::move(child)},
          skipNum{std::move(skipNum)}, limitNum{std::move(limitNum)} {}

    f_group_pos_set getGroupsPosToFlatten();

    void computeFactorizedSchema() override { copyChildSchema(0); }
    void computeFlatSchema() override { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override;

    bool hasSkipNum() const { return skipNum != nullptr; }
    bool canEvaluateSkipNum() const;
    std::shared_ptr<binder::Expression> getSkipNum() const { return skipNum; }
    uint64_t evaluateSkipNum() const;
    bool hasLimitNum() const { return limitNum != nullptr; }
    bool canEvaluateLimitNum() const;
    std::shared_ptr<binder::Expression> getLimitNum() const { return limitNum; }
    uint64_t evaluateLimitNum() const;

    f_group_pos getGroupPosToSelect() const;

    std::unordered_set<f_group_pos> getGroupsPosToLimit() const {
        return schema->getGroupsPosInScope();
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLimit>(skipNum, limitNum, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> skipNum;
    std::shared_ptr<binder::Expression> limitNum;
};

} // namespace planner
} // namespace kuzu
