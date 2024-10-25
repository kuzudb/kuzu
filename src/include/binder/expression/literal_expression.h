#pragma once

#include "common/types/value/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class KUZU_API LiteralExpression final : public Expression {
public:
    LiteralExpression(common::Value value, const std::string& uniqueName)
        : Expression{common::ExpressionType::LITERAL, value.getDataType().copy(), uniqueName},
          value{std::move(value)} {}

    bool isNull() const { return value.isNull(); }

    void cast(const common::LogicalType& type) override;

    common::Value getValue() const { return value; }

    std::string toStringInternal() const override { return value.toString(); }

    std::unique_ptr<Expression> copy() const override {
        return std::make_unique<LiteralExpression>(value, uniqueName);
    }

public:
    common::Value value;
};

} // namespace binder
} // namespace kuzu
