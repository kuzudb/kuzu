#pragma once

#include "function/gds_function.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalGDSCall final : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::GDS_CALL;

public:
    LogicalGDSCall(function::GDSFunction func, std::shared_ptr<binder::Expression> graphExpr,
        binder::expression_vector outExprs)
        : LogicalOperator{operatorType_}, func{std::move(func)}, graphExpr{std::move(graphExpr)},
          outExprs{std::move(outExprs)} {}

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    function::GDSFunction getFunction() const { return func; }
    std::shared_ptr<binder::Expression> getGraphExpr() const { return graphExpr; }
    binder::expression_vector getOutExprs() const { return outExprs; }

    std::string getExpressionsForPrinting() const override { return func.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalGDSCall>(func, graphExpr, outExprs);
    }

private:
    function::GDSFunction func;
    std::shared_ptr<binder::Expression> graphExpr;
    binder::expression_vector outExprs;
};

} // namespace planner
} // namespace kuzu
