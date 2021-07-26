#pragma once

#include <utility>

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

/**
 * Variable expression can be node, rel or csv line extract. csv line extract should be removed once
 * list expression is implemented.
 */
class VariableExpression : public Expression {

public:
    VariableExpression(ExpressionType expressionType, DataType dataType, string uniqueName)
        : Expression{expressionType, dataType}, uniqueName{move(uniqueName)} {}

    string getInternalName() const override { return uniqueName; }

protected:
    string uniqueName;
};

} // namespace binder
} // namespace graphflow
