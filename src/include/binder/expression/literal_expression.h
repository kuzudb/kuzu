#pragma once

#include "common/type_utils.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class LiteralExpression : public Expression {

public:
    LiteralExpression(DataType dataType, unique_ptr<Literal> literal)
        : Expression(LITERAL, std::move(dataType), "_" + TypeUtils::toString(*literal)),
          literal{std::move(literal)} {
        assert(!isNull());
    }

    LiteralExpression(DataTypeID dataTypeID, unique_ptr<Literal> literal)
        : LiteralExpression{DataType(dataTypeID), std::move(literal)} {
        assert(dataTypeID != LIST);
    }

    LiteralExpression(DataType dataType, unique_ptr<Literal> literal, string uniqueName)
        : Expression{LITERAL, std::move(dataType), std::move(uniqueName)}, literal{std::move(
                                                                               literal)} {}

    inline bool isNull() const { return literal->isNull(); }

    inline void setDataType(const DataType& targetType) {
        assert(dataType.typeID == ANY && isNull());
        dataType = targetType;
        literal->dataType = targetType;
    }

public:
    unique_ptr<Literal> literal;
};

} // namespace binder
} // namespace kuzu
