#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/expression_type.h"

namespace kuzu {
namespace parser {

class ParsedExpression;
using parsed_expression_vector = std::vector<std::unique_ptr<ParsedExpression>>;

class ParsedExpression {
public:
    ParsedExpression(
        common::ExpressionType type, std::unique_ptr<ParsedExpression> child, std::string rawName);

    ParsedExpression(common::ExpressionType type, std::unique_ptr<ParsedExpression> left,
        std::unique_ptr<ParsedExpression> right, std::string rawName);

    virtual ~ParsedExpression() = default;

    inline common::ExpressionType getExpressionType() const { return type; }

    inline void setAlias(std::string name) { alias = std::move(name); }

    inline bool hasAlias() const { return !alias.empty(); }

    inline std::string getAlias() const { return alias; }

    inline std::string getRawName() const { return rawName; }

    inline uint32_t getNumChildren() const { return children.size(); }

    inline ParsedExpression* getChild(uint32_t idx) const { return children[idx].get(); }

protected:
    ParsedExpression(common::ExpressionType type, std::string rawName)
        : type{type}, rawName{std::move(rawName)} {}

protected:
    common::ExpressionType type;
    std::string alias;
    std::string rawName;
    parsed_expression_vector children;
};

} // namespace parser
} // namespace kuzu
