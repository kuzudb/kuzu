#pragma once

#include "src/planner/include/bound_statements/bound_return_statement.h"

namespace graphflow {
namespace planner {

/**
 * BoundWithStatement may not have whereExpression
 */
class BoundWithStatement : public BoundReturnStatement {

public:
    explicit BoundWithStatement(vector<shared_ptr<LogicalExpression>> expressions)
        : BoundReturnStatement{move(expressions)} {}

public:
    shared_ptr<LogicalExpression> whereExpression;
};

} // namespace planner
} // namespace graphflow
