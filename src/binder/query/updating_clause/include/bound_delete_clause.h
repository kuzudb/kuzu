#pragma once

#include "bound_updating_clause.h"

namespace graphflow {
namespace binder {

class BoundDeleteClause : public BoundUpdatingClause {

public:
    BoundDeleteClause() : BoundUpdatingClause{ClauseType::DELETE} {};
    ~BoundDeleteClause() override = default;

    inline void addExpression(shared_ptr<Expression> expression) {
        expressions.push_back(move(expression));
    }

    inline expression_vector getPropertiesToRead() const override {
        expression_vector result;
        for (auto& expression : expressions) {
            for (auto& property : expression->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
        return result;
    }

    inline unique_ptr<BoundUpdatingClause> copy() override {
        auto result = make_unique<BoundDeleteClause>();
        for (auto& expression : expressions) {
            result->addExpression(expression);
        }
        return result;
    }

private:
    vector<shared_ptr<Expression>> expressions;
};

} // namespace binder
} // namespace graphflow
