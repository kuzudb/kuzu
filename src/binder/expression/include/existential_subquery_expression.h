#pragma once

#include "expression.h"

#include "src/binder/query/include/normalized_single_query.h"

namespace graphflow {
namespace binder {

class ExistentialSubqueryExpression : public Expression {

public:
    ExistentialSubqueryExpression(unique_ptr<NormalizedSingleQuery> subQuery, const string& name)
        : Expression{EXISTENTIAL_SUBQUERY, BOOL, name}, subQuery{move(subQuery)} {}

    inline NormalizedSingleQuery* getSubquery() const { return subQuery.get(); }

    unordered_set<string> getDependentVariableNames() override;

    vector<shared_ptr<Expression>> getChildren() const override;

private:
    unique_ptr<NormalizedSingleQuery> subQuery;
};

} // namespace binder
} // namespace graphflow
