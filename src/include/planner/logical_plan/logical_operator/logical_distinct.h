#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace kuzu {
namespace planner {

class LogicalDistinct : public LogicalOperator {
public:
    LogicalDistinct(
        binder::expression_vector expressionsToDistinct, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DISTINCT, std::move(child)},
          expressionsToDistinct{std::move(expressionsToDistinct)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupsPosToFlatten();

    std::string getExpressionsForPrinting() const override;

    inline binder::expression_vector getExpressionsToDistinct() const {
        return expressionsToDistinct;
    }
    inline Schema* getSchemaBeforeDistinct() const { return children[0]->getSchema(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDistinct>(expressionsToDistinct, children[0]->copy());
    }

private:
    binder::expression_vector expressionsToDistinct;
};

} // namespace planner
} // namespace kuzu
