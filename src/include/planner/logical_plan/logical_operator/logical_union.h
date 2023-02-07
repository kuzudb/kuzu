#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalUnion : public LogicalOperator {
public:
    LogicalUnion(binder::expression_vector expressions,
        std::vector<std::shared_ptr<LogicalOperator>> children)
        : LogicalOperator{LogicalOperatorType::UNION_ALL, std::move(children)},
          expressionsToUnion{std::move(expressions)} {}

    void computeSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline binder::expression_vector getExpressionsToUnion() { return expressionsToUnion; }

    inline Schema* getSchemaBeforeUnion(uint32_t idx) { return children[idx]->getSchema(); }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    binder::expression_vector expressionsToUnion;
};

} // namespace planner
} // namespace kuzu
