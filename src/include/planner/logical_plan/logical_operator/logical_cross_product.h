#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
public:
    LogicalCrossProduct(unique_ptr<Schema> buildSideSchema,
        shared_ptr<LogicalOperator> probeSideChild, shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{std::move(probeSideChild), std::move(buildSideChild)},
          buildSideSchema{std::move(buildSideSchema)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_CROSS_PRODUCT;
    }

    inline string getExpressionsForPrinting() const override { return string(); }

    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(
            buildSideSchema->copy(), children[0]->copy(), children[1]->copy());
    }

private:
    unique_ptr<Schema> buildSideSchema;
};

} // namespace planner
} // namespace kuzu
