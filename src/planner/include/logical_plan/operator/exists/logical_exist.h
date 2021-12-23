#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalExists : public LogicalOperator {

public:
    LogicalExists(shared_ptr<Expression> subqueryExpression,
        shared_ptr<LogicalOperator> subPlanLastOperator, unique_ptr<Schema> subPlanSchema,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, subqueryExpression{move(subqueryExpression)},
          subPlanLastOperator{move(subPlanLastOperator)}, subPlanSchema{move(subPlanSchema)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_EXISTS; }

    string toString(uint64_t depth = 0) const override {
        string result = LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
                        getExpressionsForPrinting() + "]\n";
        result += prevOperator->toString(depth + 1);
        result += "\nSUBPLAN: \n";
        result += subPlanLastOperator->toString(depth + 1);
        return result;
    }

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExists>(subqueryExpression, subPlanLastOperator->copy(),
            subPlanSchema->copy(), prevOperator->copy());
    }

public:
    shared_ptr<Expression> subqueryExpression;
    shared_ptr<LogicalOperator> subPlanLastOperator;
    unique_ptr<Schema> subPlanSchema;
};

} // namespace planner
} // namespace graphflow
