#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

struct CaseAlternative {
    shared_ptr<Expression> whenExpression;
    shared_ptr<Expression> thenExpression;

    CaseAlternative(shared_ptr<Expression> whenExpression, shared_ptr<Expression> thenExpression)
        : whenExpression{std::move(whenExpression)}, thenExpression{std::move(thenExpression)} {}
};

class CaseExpression : public Expression {
public:
    CaseExpression(DataType dataType, shared_ptr<Expression> elseExpression, string name)
        : Expression{CASE_ELSE, dataType, name}, elseExpression{std::move(elseExpression)} {}

    inline void addCaseAlternative(shared_ptr<Expression> when, shared_ptr<Expression> then) {
        caseAlternatives.push_back(make_unique<CaseAlternative>(std::move(when), std::move(then)));
    }
    inline size_t getNumCaseAlternatives() const { return caseAlternatives.size(); }
    inline CaseAlternative* getCaseAlternative(size_t idx) const {
        return caseAlternatives[idx].get();
    }

    inline shared_ptr<Expression> getElseExpression() const { return elseExpression; }

    expression_vector getChildren() const override;

private:
    vector<unique_ptr<CaseAlternative>> caseAlternatives;
    shared_ptr<Expression> elseExpression;
};

} // namespace binder
} // namespace kuzu
