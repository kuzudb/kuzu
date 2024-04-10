#pragma once

#include <utility>

#include "common/enums/explain_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalExplain : public LogicalOperator {
public:
    LogicalExplain(std::shared_ptr<LogicalOperator> child,
        std::shared_ptr<binder::Expression> outputExpression, common::ExplainType explainType,
        binder::expression_vector outputExpressionsToExplain)
        : LogicalOperator{LogicalOperatorType::EXPLAIN, std::move(child)},
          outputExpression{std::move(outputExpression)}, explainType{explainType},
          outputExpressionsToExplain{std::move(outputExpressionsToExplain)} {}

    void computeSchema();
    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline std::string getExpressionsForPrinting() const override { return "Explain"; }

    inline common::ExplainType getExplainType() const { return explainType; }

    inline binder::expression_vector getOutputExpressionsToExplain() const {
        return outputExpressionsToExplain;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalExplain>(children[0], outputExpression, explainType,
            outputExpressionsToExplain);
    }

private:
    std::shared_ptr<binder::Expression> outputExpression;
    common::ExplainType explainType;
    binder::expression_vector outputExpressionsToExplain;
};

} // namespace planner
} // namespace kuzu
