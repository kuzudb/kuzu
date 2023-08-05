#pragma once

#include "base_logical_operator.h"
#include "binder/expression/literal_expression.h"

namespace kuzu {
namespace planner {

class LogicalDummyScan : public LogicalOperator {
public:
    LogicalDummyScan() : LogicalOperator{LogicalOperatorType::DUMMY_SCAN} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    static std::shared_ptr<binder::Expression> getDummyExpression();

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalDummyScan>();
    }
};

} // namespace planner
} // namespace kuzu
