#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {
public:
    ParsedPropertyExpression(
        std::string propertyName, std::unique_ptr<ParsedExpression> child, std::string raw)
        : ParsedExpression{common::ExpressionType::PROPERTY, std::move(child), std::move(raw)},
          propertyName{std::move(propertyName)} {}

    ParsedPropertyExpression(common::ExpressionType type, std::string alias, std::string rawName,
        parsed_expression_vector children, std::string propertyName)
        : ParsedExpression{type, std::move(alias), std::move(rawName), std::move(children)},
          propertyName{std::move(propertyName)} {}

    inline std::string getPropertyName() const { return propertyName; }
    inline bool isStar() const { return propertyName == common::InternalKeyword::STAR; }

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedPropertyExpression>(
            type, alias, rawName, copyChildren(), propertyName);
    }

private:
    std::string propertyName;
};

} // namespace parser
} // namespace kuzu
