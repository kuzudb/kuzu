#pragma once

#include "src/binder/include/expression/variable_expression.h"

namespace graphflow {
namespace binder {

const string INTERNAL_ID_SUFFIX = "_id";

class NodeExpression : public VariableExpression {

public:
    NodeExpression(const string& nodeName, uint32_t id, label_t label)
        : VariableExpression{VARIABLE, NODE, nodeName, id}, label{label} {}

    inline string getIDProperty() { return uniqueName + "." + INTERNAL_ID_SUFFIX; }

public:
    label_t label;
};

} // namespace binder
} // namespace graphflow
