#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
public:
    LogicalCrossProduct(unique_ptr<Schema> buildSideSchema,
        vector<uint64_t> flatOutputGroupPositions, shared_ptr<LogicalOperator> probeSideChild,
        shared_ptr<LogicalOperator> buildSideChild)
        : LogicalOperator{std::move(probeSideChild), std::move(buildSideChild)},
          buildSideSchema{std::move(buildSideSchema)}, flatOutputGroupPositions{
                                                           std::move(flatOutputGroupPositions)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_CROSS_PRODUCT;
    }

    inline string getExpressionsForPrinting() const override { return string(); }

    inline Schema* getBuildSideSchema() const { return buildSideSchema.get(); }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(buildSideSchema->copy(), flatOutputGroupPositions,
            children[0]->copy(), children[1]->copy());
    }

private:
    unique_ptr<Schema> buildSideSchema;
    vector<uint64_t> flatOutputGroupPositions;
};

} // namespace planner
} // namespace kuzu
