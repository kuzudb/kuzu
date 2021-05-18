#pragma once

#include "src/binder/include/expression/node_expression.h"

namespace graphflow {
namespace binder {

class RelExpression : public Expression {

public:
    RelExpression(const string& relName, label_t label) : Expression{VARIABLE, REL}, label{label} {
        variableName = relName;
    }

    inline string getSrcNodeName() const { return srcNode->variableName; }

    inline string getDstNodeName() const { return dstNode->variableName; }

    unordered_set<string> getIncludedVariableNames() const override {
        return unordered_set<string>{variableName};
    }

public:
    label_t label;
    NodeExpression* srcNode;
    NodeExpression* dstNode;
};

} // namespace binder
} // namespace graphflow
