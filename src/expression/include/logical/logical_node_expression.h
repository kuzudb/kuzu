#pragma once

#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

class LogicalNodeExpression : public LogicalExpression {

public:
    LogicalNodeExpression(const string& name, label_t label)
        : LogicalExpression{VARIABLE, NODE, name}, label{label} {}

    unordered_set<string> getIncludedVariables() const override {
        return unordered_set<string>{variableName};
    }

public:
    label_t label;
};

} // namespace expression
} // namespace graphflow
