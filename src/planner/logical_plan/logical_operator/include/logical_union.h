#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalUnion : public LogicalOperator {

public:
    explicit LogicalUnion(expression_vector expressions) : expressionsToUnion{move(expressions)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_UNION_ALL;
    }

    string getExpressionsForPrinting() const override;

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalUnion>(expressionsToUnion);
    }

    inline expression_vector getExpressionsToUnion() { return expressionsToUnion; }

private:
    expression_vector expressionsToUnion;
};

} // namespace planner
} // namespace graphflow
