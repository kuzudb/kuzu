#pragma once

#include "base_logical_operator.h"
#include "sink_util.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
public:
    LogicalCrossProduct(std::shared_ptr<LogicalOperator> probeSideChild,
        std::shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{LogicalOperatorType::CROSS_PRODUCT, std::move(probeSideChild),
              std::move(buildSideChild)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline Schema* getBuildSideSchema() const { return children[1]->getSchema(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(children[0]->copy(), children[1]->copy());
    }
};

} // namespace planner
} // namespace kuzu
