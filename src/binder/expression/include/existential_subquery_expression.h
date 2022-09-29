#pragma once

#include "expression.h"

#include "src/binder/query/reading_clause/include/bound_match_clause.h"

namespace graphflow {
namespace binder {

class ExistentialSubqueryExpression : public Expression {

public:
    ExistentialSubqueryExpression(unique_ptr<QueryGraph> queryGraph, const string& name)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL, name}, queryGraph{std::move(queryGraph)} {}

    inline QueryGraph* getQueryGraph() const { return queryGraph.get(); }

    inline void setWhereExpression(shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    unordered_set<string> getDependentVariableNames() override;

    expression_vector getChildren() const override;

private:
    unique_ptr<QueryGraph> queryGraph;
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace graphflow
