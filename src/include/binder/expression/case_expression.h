#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

struct CaseAlternative {
    std::shared_ptr<Expression> whenExpression;
    std::shared_ptr<Expression> thenExpression;

    CaseAlternative(
        std::shared_ptr<Expression> whenExpression, std::shared_ptr<Expression> thenExpression)
        : whenExpression{std::move(whenExpression)}, thenExpression{std::move(thenExpression)} {}
};

class CaseExpression : public Expression {
public:
    CaseExpression(common::LogicalType dataType, std::shared_ptr<Expression> elseExpression,
        const std::string& name)
        : Expression{common::ExpressionType::CASE_ELSE, std::move(dataType), name},
          elseExpression{std::move(elseExpression)} {}

    inline void addCaseAlternative(
        std::shared_ptr<Expression> when, std::shared_ptr<Expression> then) {
        caseAlternatives.push_back(make_unique<CaseAlternative>(std::move(when), std::move(then)));
    }
    inline size_t getNumCaseAlternatives() const { return caseAlternatives.size(); }
    inline CaseAlternative* getCaseAlternative(size_t idx) const {
        return caseAlternatives[idx].get();
    }

    inline std::shared_ptr<Expression> getElseExpression() const { return elseExpression; }

    std::string toStringInternal() const final;

private:
    std::vector<std::unique_ptr<CaseAlternative>> caseAlternatives;
    std::shared_ptr<Expression> elseExpression;
};

} // namespace binder
} // namespace kuzu
