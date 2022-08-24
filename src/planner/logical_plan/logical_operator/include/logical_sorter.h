#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/node_expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalSorter : public LogicalOperator {

public:
    LogicalSorter(shared_ptr<NodeExpression> expressionToSort, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expressionToSort{move(expressionToSort)} {}

    inline string getExpressionsForPrinting() const override {
        return expressionToSort->getUniqueName() + "_" + INTERNAL_ID_SUFFIX;
    }
    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SORTER;
    }
    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSorter>(expressionToSort, children[0]->copy());
    }

public:
    shared_ptr<NodeExpression> expressionToSort;
};

} // namespace planner
} // namespace graphflow
