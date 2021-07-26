#pragma once

#include "src/parser/include/parsed_expression.h"

namespace graphflow {
namespace parser {

class ReturnStatement {

public:
    ReturnStatement(vector<unique_ptr<ParsedExpression>> expressions, bool containsStar)
        : containsStar{containsStar}, expressions{move(expressions)} {}

    virtual ~ReturnStatement() = default;

public:
    bool containsStar;
    vector<unique_ptr<ParsedExpression>> expressions;
};

} // namespace parser
} // namespace graphflow
