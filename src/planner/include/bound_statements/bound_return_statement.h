#pragma once

#include "src/expression/include/logical/logical_expression.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class BoundReturnStatement {

public:
    explicit BoundReturnStatement(vector<shared_ptr<LogicalExpression>> expressions)
        : expressions{move(expressions)} {}

public:
    vector<shared_ptr<LogicalExpression>> expressions;
};

} // namespace planner
} // namespace graphflow
