#pragma once

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace binder {

class ExpressionRewriter {
public:
    static shared_ptr<Expression> rewrite(shared_ptr<Expression> expression) {
        // Rewrite constant
        // Any function applies on constant children should be evaluated statically,
        // e.g. 1 + 1, date('2022-10-10') ...

        // Rewrite boolean (check truth table in boolean_operations.h)
        // e.g. X AND T = X
        // e.g. X OR T = T

        // Rewrite boolean distributive
        // e.g (X AND B) OR (X AND C) OR (X AND D) = X AND (B OR C OR D)
        // This helpers with filter push down

        // Rewrite arithmetic
        // e.g. a + 1 = 2  -> a = 1
        // e.g. a * 0 = 0

        // Rewrite null
        // Except for IS NULL, IS NOT NULL, and boolean operations, any operation on NULL should
        // return NULL.
        // e.g. a + NULL -> NULL
        return expression;
    }
};

} // namespace binder
} // namespace graphflow
