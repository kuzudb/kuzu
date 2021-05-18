#pragma once

#include "src/binder/include/bound_statements/bound_return_statement.h"

namespace graphflow {
namespace binder {

/**
 * BoundWithStatement may not have whereExpression
 */
class BoundWithStatement : public BoundReturnStatement {

public:
    explicit BoundWithStatement(vector<shared_ptr<Expression>> expressions)
        : BoundReturnStatement{move(expressions)} {}

public:
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace graphflow
