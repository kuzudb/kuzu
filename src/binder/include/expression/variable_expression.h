#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

/**
 * Variable expression can be node, rel or csv line extract. csv line extract should be removed once
 * list expression is implemented.
 */
class VariableExpression : public Expression {

public:
    VariableExpression(
        ExpressionType expressionType, DataType dataType, const string& name, uint32_t id)
        : Expression{expressionType, dataType} {
        makeUniqueName(name, id);
    }

    string getInternalName() const override { return uniqueName; }

private:
    void makeUniqueName(const string& name, uint32_t id) {
        uniqueName = "_" + to_string(id) + "_" + name;
    }

protected:
    string uniqueName;
};

} // namespace binder
} // namespace graphflow
