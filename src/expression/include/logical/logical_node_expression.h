#pragma once

#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

const string INTERNAL_ID = "_id";

class LogicalNodeExpression : public LogicalExpression {

public:
    LogicalNodeExpression(const string& name, label_t label)
        : LogicalExpression{VARIABLE, NODE, name}, label{label} {}

    unordered_set<string> getIncludedVariables() const override {
        return unordered_set<string>{variableName};
    }

    string getIDProperty() { return variableName + "." + INTERNAL_ID; }

public:
    label_t label;
};

} // namespace expression
} // namespace graphflow
