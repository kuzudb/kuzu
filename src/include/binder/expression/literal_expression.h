#pragma once

#include "common/types/value/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class LiteralExpression : public Expression {
public:
    LiteralExpression(std::unique_ptr<common::Value> value, const std::string& uniqueName)
        : Expression{common::ExpressionType::LITERAL, *value->getDataType(), uniqueName},
          value{std::move(value)} {}

    inline bool isNull() const { return value->isNull(); }

    inline void setDataType(const common::LogicalType& targetType) {
        KU_ASSERT(dataType.getLogicalTypeID() == common::LogicalTypeID::ANY && isNull());
        dataType = targetType;
        value->setDataType(targetType);
    }

    inline common::Value* getValue() const { return value.get(); }

    std::string toStringInternal() const final { return value->toString(); }

    inline std::unique_ptr<Expression> copy() const final {
        return std::make_unique<LiteralExpression>(value->copy(), uniqueName);
    }

public:
    std::unique_ptr<common::Value> value;
};

} // namespace binder
} // namespace kuzu
