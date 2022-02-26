#pragma once

#include "base_logical_operator.h"
#include "schema.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

// Sub-plan last operator on right, i.e. children[1].
class LogicalExists : public LogicalOperator {

public:
    LogicalExists(shared_ptr<Expression> subqueryExpression, unique_ptr<Schema> subPlanSchema,
        shared_ptr<LogicalOperator> child, shared_ptr<LogicalOperator> subPlanChild)
        : LogicalOperator{move(child), move(subPlanChild)},
          subqueryExpression{move(subqueryExpression)}, subPlanSchema{move(subPlanSchema)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_EXISTS; }

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExists>(
            subqueryExpression, subPlanSchema->copy(), children[0]->copy(), children[1]->copy());
    }

public:
    shared_ptr<Expression> subqueryExpression;
    unique_ptr<Schema> subPlanSchema;
};

} // namespace planner
} // namespace graphflow
