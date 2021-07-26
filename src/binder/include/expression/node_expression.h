#pragma once

#include "src/binder/include/expression/variable_expression.h"

namespace graphflow {
namespace binder {

const string INTERNAL_ID_SUFFIX = "_id";

class NodeExpression : public VariableExpression {

public:
    NodeExpression(const string& uniqueName, label_t label)
        : VariableExpression{VARIABLE, NODE, uniqueName}, label{label} {}

    inline string getIDProperty() const { return uniqueName + "." + INTERNAL_ID_SUFFIX; }

public:
    label_t label;
};

} // namespace binder
} // namespace graphflow
