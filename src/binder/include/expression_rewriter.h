#pragma once

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace binder {

class ExpressionRewriter {
public:
    static shared_ptr<Expression> rewrite(shared_ptr<Expression> expression) {
        return expression;
    }
};

} // namespace binder
} // namespace graphflow
