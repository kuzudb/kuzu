#pragma once

#include "binder/expression/expression.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalFilterPrintInfo final : OPPrintInfo {
    std::string expression;

    explicit LogicalFilterPrintInfo(std::string expression) : expression(std::move(expression)) {}

    std::string toString() const override { return "Filter: " + expression; }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<LogicalFilterPrintInfo>(expression);
    }
};

class LogicalFilter final : public LogicalOperator {
public:
    LogicalFilter(std::shared_ptr<binder::Expression> expression,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FILTER, std::move(child)},
          expression{std::move(expression)} {}

    void computeFactorizedSchema() override { copyChildSchema(0); }
    void computeFlatSchema() override { copyChildSchema(0); }

    f_group_pos_set getGroupsPosToFlatten();

    std::string getExpressionsForPrinting() const override { return expression->toString(); }

    std::shared_ptr<binder::Expression> getPredicate() const { return expression; }

    f_group_pos getGroupPosToSelect() const;

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalFilterPrintInfo>(expression->toString());
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFilter>(expression, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> expression;
};

} // namespace planner
} // namespace kuzu
