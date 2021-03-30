#pragma once

#include "src/common/include/types.h"
#include "src/expression/include/logical/logical_expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::expression;
using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalFilter : public LogicalOperator {

public:
    LogicalFilter(unique_ptr<LogicalExpression> rootExpr, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, rootExpr{move(rootExpr)} {}

    LogicalOperatorType getLogicalOperatorType() const {
        return LogicalOperatorType::LOGICAL_FILTER;
    }

    const LogicalExpression& getRootLogicalExpression() const { return *rootExpr; }

public:
    unique_ptr<LogicalExpression> rootExpr;
};

} // namespace planner
} // namespace graphflow
