#pragma once

#include <memory>
#include <string>
#include <vector>

#include "src/common/include/expression_type.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace parser {

class ParsedExpression {

public:
    ParsedExpression(ExpressionType type, string text, string rawExpression)
        : type{type}, text{move(text)}, rawExpression{move(rawExpression)} {}

    ParsedExpression(ExpressionType type, string text, string rawExpression,
        unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right)
        : type{type}, text{move(text)}, rawExpression{move(rawExpression)} {
        children.push_back(move(left));
        children.push_back(move(right));
    }

    // no need to compare rawExpression
    bool operator==(const ParsedExpression& other) {
        auto result = type == other.type && text == other.text;
        for (auto i = 0ul; i < children.size(); ++i) {
            result &= *children[i] == *other.children[i];
        }
        return result;
    }

public:
    ExpressionType type;
    string text;
    string rawExpression;
    vector<unique_ptr<ParsedExpression>> children;
};

} // namespace parser
} // namespace graphflow
