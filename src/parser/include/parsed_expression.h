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
    explicit ParsedExpression(ExpressionType type) : type{type} {}

    ParsedExpression(ExpressionType type, string text) : type{type}, text{move(text)} {}

    ParsedExpression(
        ExpressionType type, unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right)
        : type{type} {
        children.push_back(move(left));
        children.push_back(move(right));
    }

    void addChild(unique_ptr<ParsedExpression> child) { children.push_back(move(child)); }

    bool operator==(const ParsedExpression& other) {
        auto result = type == other.type && text == other.text;
        for (auto i = 0ul; i < children.size(); ++i) {
            result &= *children[i] == *other.children[i];
        }
        return result;
    }

protected:
    ExpressionType type;
    string text;
    vector<unique_ptr<ParsedExpression>> children;
};

} // namespace parser
} // namespace graphflow
