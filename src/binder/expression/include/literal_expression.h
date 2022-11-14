#pragma once

#include "expression.h"

#include "src/common/include/type_utils.h"

namespace kuzu {
namespace binder {

class LiteralExpression : public Expression {

public:
    LiteralExpression(DataType dataType, unique_ptr<Literal> literal)
        : Expression(LITERAL, move(dataType), "_" + TypeUtils::toString(*literal)), literal{move(
                                                                                        literal)} {}

    LiteralExpression(DataTypeID dataTypeID, unique_ptr<Literal> literal)
        : LiteralExpression{DataType(dataTypeID), move(literal)} {
        assert(dataTypeID != LIST);
    }

    inline bool isNull() const { return literal->isNull(); }

    inline void setDataType(const DataType& targetType) {
        assert(dataType.typeID == ANY && isNull());
        dataType = targetType;
        literal->dataType = targetType;
    }

    static unique_ptr<LiteralExpression> createNullLiteralExpression(DataType dataType) {
        auto nullLiteral = make_unique<Literal>();
        nullLiteral->dataType = dataType;
        return make_unique<LiteralExpression>(dataType, std::move(nullLiteral));
    }

public:
    unique_ptr<Literal> literal;
};

} // namespace binder
} // namespace kuzu
