#pragma once

#include "src/binder/include/expression/node_expression.h"

namespace graphflow {
namespace binder {

class RelExpression : public Expression {

public:
    RelExpression(const string& relName, label_t label, uint64_t lowerBound, uint64_t upperBound)
        : Expression{VARIABLE, REL}, label{label}, lowerBound{lowerBound}, upperBound{upperBound} {
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
    uint64_t lowerBound;
    uint64_t upperBound;
};

} // namespace binder
} // namespace graphflow
