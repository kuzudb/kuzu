#pragma once

#include "planner/operator/logical_operator.h"

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
