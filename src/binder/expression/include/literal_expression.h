#pragma once

#include "expression.h"

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace binder {

class LiteralExpression : public Expression {

public:
    LiteralExpression(DataType dataType, unique_ptr<Literal> literal)
        : Expression(LITERAL, move(dataType), TypeUtils::toString(*literal)), literal{
                                                                                  move(literal)} {}

    LiteralExpression(DataTypeID dataTypeID, unique_ptr<Literal> literal)
        : LiteralExpression{DataType(dataTypeID), move(literal)} {
        assert(dataTypeID != LIST);
    }

    bool isNull() const { return literal->isNull(); }

    static unique_ptr<LiteralExpression> createNullLiteralExpression(DataType dataType) {
        auto nullLiteral = make_unique<Literal>();
        nullLiteral->dataType = dataType;
        return make_unique<LiteralExpression>(dataType, std::move(nullLiteral));
    }

public:
    unique_ptr<Literal> literal;
};

} // namespace binder
} // namespace graphflow
