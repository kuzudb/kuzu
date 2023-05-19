#pragma once

#include "common/types/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class LiteralExpression : public Expression {
public:
    LiteralExpression(std::unique_ptr<common::Value> value, const std::string& uniqueName)
        : Expression{common::LITERAL, value->getDataType(), uniqueName}, value{std::move(value)} {}

    inline bool isNull() const { return value->isNull(); }

    inline void setDataType(const common::LogicalType& targetType) {
        assert(dataType.getLogicalTypeID() == common::LogicalTypeID::ANY && isNull());
        dataType = targetType;
        value->setDataType(targetType);
    }

    inline common::Value* getValue() const { return value.get(); }

    std::string toString() const override { return value->toString(); }

public:
    std::unique_ptr<common::Value> value;
};

} // namespace binder
} // namespace kuzu
