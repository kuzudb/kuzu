#pragma once

#include "base_logical_operator.h"
#include "sink_util.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
public:
    LogicalCrossProduct(
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{LogicalOperatorType::CROSS_PRODUCT, std::move(probeSideChild),
              std::move(buildSideChild)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override { return string(); }

    inline Schema* getBuildSideSchema() const { return children[1]->getSchema(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(children[0]->copy(), children[1]->copy());
    }
};

} // namespace planner
} // namespace kuzu
