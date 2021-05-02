#pragma once

#include "src/expression/include/logical/logical_node_expression.h"

namespace graphflow {
namespace expression {

class LogicalRelExpression : public LogicalExpression {

public:
    LogicalRelExpression(string name, label_t label)
        : LogicalExpression{VARIABLE, REL}, name{move(name)}, label{label} {}

    inline string getSrcNodeName() const { return srcNode->name; }

    inline string getDstNodeName() const { return dstNode->name; }

    unordered_set<string> getIncludedVariables() const override {
        return unordered_set<string>{name};
    }

public:
    string name;
    label_t label;
    LogicalNodeExpression* srcNode;
    LogicalNodeExpression* dstNode;
};

} // namespace expression
} // namespace graphflow
