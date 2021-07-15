#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/common/include/function.h"

namespace graphflow {
namespace binder {

class FunctionExpression : public Expression {

public:
    FunctionExpression(const Function& function)
        : Expression{FUNCTION, function.returnType}, function{function} {}

public:
    const Function& function;
};

} // namespace binder
} // namespace graphflow
