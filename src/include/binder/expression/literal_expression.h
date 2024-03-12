#pragma once

#include "common/types/value/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class LiteralExpression final : public Expression {
public:
    LiteralExpression(std::unique_ptr<common::Value> value, const std::string& uniqueName)
        : Expression{common::ExpressionType::LITERAL, *value->getDataType(), uniqueName},
          value{std::move(value)} {}

    bool isNull() const { return value->isNull(); }

    void cast(const common::LogicalType& type) override;

    common::Value* getValue() const { return value.get(); }

    std::string toStringInternal() const final { return value->toString(); }

    std::unique_ptr<Expression> copy() const final {
        return std::make_unique<LiteralExpression>(value->copy(), uniqueName);
    }

public:
    std::unique_ptr<common::Value> value;
};

} // namespace binder
} // namespace kuzu
