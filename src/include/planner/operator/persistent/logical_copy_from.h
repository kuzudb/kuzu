#pragma once

#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyFrom final : public LogicalOperator {
public:
    LogicalCopyFrom(binder::BoundCopyFromInfo info, binder::expression_vector outExprs,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::COPY_FROM, std::move(child)}, info{std::move(info)},
          outExprs{std::move(outExprs)} {}
    LogicalCopyFrom(binder::BoundCopyFromInfo info, binder::expression_vector outExprs,
        logical_op_vector_t children)
        : LogicalOperator{LogicalOperatorType::COPY_FROM, std::move(children)},
          info{std::move(info)}, outExprs{std::move(outExprs)} {}

    inline std::string getExpressionsForPrinting() const override {
        return info.tableEntry->getName();
    }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline const binder::BoundCopyFromInfo* getInfo() const { return &info; }
    inline binder::expression_vector getOutExprs() const { return outExprs; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyFrom>(info.copy(), outExprs, LogicalOperator::copy(children));
    }

private:
    binder::BoundCopyFromInfo info;
    binder::expression_vector outExprs;
};

} // namespace planner
} // namespace kuzu
