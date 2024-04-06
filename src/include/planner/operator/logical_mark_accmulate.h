#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalMarkAccumulate final : public LogicalOperator {
public:
    LogicalMarkAccumulate(binder::expression_vector keys, std::shared_ptr<binder::Expression> mark,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::MARK_ACCUMULATE, std::move(child)},
          keys{std::move(keys)}, mark{std::move(mark)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupsPosToFlatten() const;

    std::string getExpressionsForPrinting() const override { return {}; }
    binder::expression_vector getKeys() const { return keys; }
    binder::expression_vector getPayloads() const;
    std::shared_ptr<binder::Expression> getMark() const { return mark; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalMarkAccumulate>(keys, mark, children[0]->copy());
    }

private:
    binder::expression_vector keys;
    std::shared_ptr<binder::Expression> mark;
};

} // namespace planner
} // namespace kuzu
