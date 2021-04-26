#pragma once

#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

class LogicalNodeExpression : public LogicalExpression {

public:
    LogicalNodeExpression(string name, label_t label)
        : LogicalExpression{VARIABLE, NODE}, name{move(name)}, label{label} {}

public:
    string name;
    label_t label;
};

} // namespace expression
} // namespace graphflow
