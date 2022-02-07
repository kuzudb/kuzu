#pragma once

#include <memory>
#include <string>
#include <vector>

#include "src/common/include/expression_type.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace parser {

class SingleQuery;

class ParsedExpression {

public:
    ParsedExpression(ExpressionType type, string text, string rawName)
        : type{type}, text{move(text)}, rawName{move(rawName)} {}

    ParsedExpression(ExpressionType type, unique_ptr<SingleQuery> subquery, string rawName)
        : type{type}, subquery{move(subquery)}, rawName{move(rawName)} {};

    ParsedExpression(ExpressionType type, string text, string rawName,
        unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right)
        : type{type}, text{move(text)}, rawName{move(rawName)} {
        children.push_back(move(left));
        children.push_back(move(right));
    }

    inline string getRawName() const { return rawName; }

    inline void setIsDistinct() { isDistinct = true; }
    inline bool getIsDistinct() const { return isDistinct; }

public:
    ExpressionType type;
    // variableName, propertyName or functionName
    string text;
    // existential subquery
    unique_ptr<SingleQuery> subquery;
    string alias;
    string rawName;
    vector<unique_ptr<ParsedExpression>> children;

private:
    // TODO: ParsedExpression is getting bulky, we should start creating special parsed expressions,
    // e.g. function expression.
    bool isDistinct = false;
};

} // namespace parser
} // namespace graphflow
