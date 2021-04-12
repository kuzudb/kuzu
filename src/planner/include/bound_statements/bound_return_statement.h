#pragma once

#include "src/expression/include/logical/logical_expression.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class BoundReturnStatement {

public:
    vector<shared_ptr<LogicalExpression>> expressionsToReturn;
};

} // namespace planner
} // namespace graphflow
