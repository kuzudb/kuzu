#pragma once

#include "src/expression/include/logical/logical_node_expression.h"

namespace graphflow {
namespace expression {

class LogicalRelExpression : public LogicalExpression {

public:
    LogicalRelExpression(const string& name, label_t label)
        : LogicalExpression{VARIABLE, REL, name}, label{label} {}

    inline string getSrcNodeName() const { return srcNode->variableName; }

    inline string getDstNodeName() const { return dstNode->variableName; }

    unordered_set<string> getIncludedVariables() const override {
        return unordered_set<string>{variableName};
    }

public:
    label_t label;
    LogicalNodeExpression* srcNode;
    LogicalNodeExpression* dstNode;
};

} // namespace expression
} // namespace graphflow
