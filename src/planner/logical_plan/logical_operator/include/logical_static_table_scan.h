#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalStaticTableScan : public LogicalOperator {
public:
    // Table scan does not take input from child operator. So its input expressions must be
    // evaluated statically i.e. must be literal.
    LogicalStaticTableScan(expression_vector expressions) : expressions{move(expressions)} {
        for (auto& expression : this->expressions) {
            assert(expression->expressionType == LITERAL);
        }
    }

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_STATIC_TABLE_SCAN;
    }

    inline string getExpressionsForPrinting() const override { return string(); }
    inline expression_vector getExpressions() const { return expressions; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalStaticTableScan>(expressions);
    }

private:
    expression_vector expressions;
};

} // namespace planner
} // namespace graphflow
