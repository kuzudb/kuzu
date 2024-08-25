#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class AliasExpression final : public Expression {
    static constexpr common::ExpressionType expressionType_ = common::ExpressionType::ALIAS;

public:
    AliasExpression(std::string aliasName, std::shared_ptr<Expression> origin, std::string uniqueName)
        : Expression{expressionType_, origin->getDataType().copy(), uniqueName},
          aliasName{aliasName}, origin{std::move(origin)} {}

    std::string getAliasName() const { return aliasName; }
    std::shared_ptr<Expression> getOrigin() const { return origin; }

    std::string toStringInternal() const override { return aliasName; }

private:
    std::string aliasName;
    std::shared_ptr<Expression> origin;
};

}
}
