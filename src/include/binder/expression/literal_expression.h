#pragma once

#include "common/types/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class LiteralExpression : public Expression {
public:
    LiteralExpression(unique_ptr<Value> value, const string& uniqueName)
        : Expression{LITERAL, value->getDataType(), uniqueName}, value{std::move(value)} {}

    inline bool isNull() const { return value->isNull(); }

    inline void setDataType(const DataType& targetType) {
        assert(dataType.typeID == ANY && isNull());
        dataType = targetType;
        value->setDataType(targetType);
    }

    inline Value* getValue() const { return value.get(); }

public:
    unique_ptr<Value> value;
};

} // namespace binder
} // namespace kuzu
