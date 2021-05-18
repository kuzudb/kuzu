#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class BoundReturnStatement {

public:
    explicit BoundReturnStatement(vector<shared_ptr<Expression>> expressions)
        : expressions{move(expressions)} {}

public:
    vector<shared_ptr<Expression>> expressions;
};

} // namespace binder
} // namespace graphflow
