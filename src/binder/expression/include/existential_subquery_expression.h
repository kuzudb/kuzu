#pragma once

#include "expression.h"

#include "src/binder/query/reading_clause/include/bound_match_clause.h"

namespace kuzu {
namespace binder {

class ExistentialSubqueryExpression : public Expression {
public:
    ExistentialSubqueryExpression(
        unique_ptr<QueryGraphCollection> queryGraphCollection, const string& name)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL, name}, queryGraphCollection{
                                                            std::move(queryGraphCollection)} {}

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    unordered_set<string> getDependentVariableNames() override;

    expression_vector getChildren() const override;

private:
    unique_ptr<QueryGraphCollection> queryGraphCollection;
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace kuzu
