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

public:
    unique_ptr<Literal> literal;
};

} // namespace binder
} // namespace graphflow
