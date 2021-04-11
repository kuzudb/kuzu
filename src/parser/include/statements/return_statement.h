#pragma once

#include "src/parser/include/parsed_expression.h"

namespace graphflow {
namespace parser {

class ReturnStatement {

public:
    ReturnStatement(vector<unique_ptr<ParsedExpression>> expressions, bool containsStar)
        : containsStar{containsStar}, expressions{move(expressions)} {}

    bool operator==(const ReturnStatement& other) {
        auto result =
            containsStar == other.containsStar && expressions.size() == other.expressions.size();
        for (auto i = 0u; i < expressions.size(); ++i) {
            result &= *expressions[i] == *other.expressions[i];
        }
        return result;
    }

public:
    bool containsStar;
    vector<unique_ptr<ParsedExpression>> expressions;
};

} // namespace parser
} // namespace graphflow
