#pragma once

#include "common/constants.h"
#include "common/ser_deser.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {
public:
    ParsedPropertyExpression(
        std::string propertyName, std::unique_ptr<ParsedExpression> child, std::string raw)
        : ParsedExpression{common::ExpressionType::PROPERTY, std::move(child), std::move(raw)},
          propertyName{std::move(propertyName)} {}

    ParsedPropertyExpression(std::string alias, std::string rawName,
        parsed_expression_vector children, std::string propertyName)
        : ParsedExpression{common::ExpressionType::PROPERTY, std::move(alias), std::move(rawName),
              std::move(children)},
          propertyName{std::move(propertyName)} {}

    explicit ParsedPropertyExpression(std::string propertyName)
        : ParsedExpression{common::ExpressionType::PROPERTY}, propertyName{
                                                                  std::move(propertyName)} {}

    inline std::string getPropertyName() const { return propertyName; }
    inline bool isStar() const { return propertyName == common::InternalKeyword::STAR; }

    static std::unique_ptr<ParsedPropertyExpression> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedPropertyExpression>(
            alias, rawName, copyChildren(), propertyName);
    }

private:
    inline void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) const override {
        common::SerDeser::serializeValue(propertyName, fileInfo, offset);
    }

private:
    std::string propertyName;
};

} // namespace parser
} // namespace kuzu
