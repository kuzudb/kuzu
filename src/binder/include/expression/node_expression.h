#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

const string INTERNAL_ID = "_id";

class NodeExpression : public Expression {

public:
    NodeExpression(const string& nodeName, label_t label)
        : Expression{VARIABLE, NODE}, label{label} {
        variableName = nodeName;
    }

    string getIDProperty() { return variableName + "." + INTERNAL_ID; }

public:
    label_t label;
};

} // namespace binder
} // namespace graphflow
