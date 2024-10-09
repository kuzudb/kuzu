#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDummyScan : public LogicalOperator {
public:
    explicit LogicalDummyScan(std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{LogicalOperatorType::DUMMY_SCAN, std::move(printInfo)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    static std::shared_ptr<binder::Expression> getDummyExpression();

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalDummyScan>(printInfo->copy());
    }
};

} // namespace planner
} // namespace kuzu
