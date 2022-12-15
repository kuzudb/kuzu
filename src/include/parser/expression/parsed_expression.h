#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/expression_type.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace parser {

class ParsedExpression {
public:
    ParsedExpression(ExpressionType type, unique_ptr<ParsedExpression> child, string rawName);

    ParsedExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
        unique_ptr<ParsedExpression> right, string rawName);

    virtual ~ParsedExpression() = default;

    inline ExpressionType getExpressionType() const { return type; }

    inline void setAlias(string name) { alias = move(name); }

    inline bool hasAlias() const { return !alias.empty(); }

    inline string getAlias() const { return alias; }

    inline string getRawName() const { return rawName; }

    inline uint32_t getNumChildren() const { return children.size(); }

    inline ParsedExpression* getChild(uint32_t idx) const { return children[idx].get(); }

protected:
    ParsedExpression(ExpressionType type, string rawName) : type{type}, rawName{move(rawName)} {}

protected:
    ExpressionType type;
    string alias;
    string rawName;
    vector<unique_ptr<ParsedExpression>> children;
};

} // namespace parser
} // namespace kuzu
