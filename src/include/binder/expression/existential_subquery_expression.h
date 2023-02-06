#pragma once

#include "binder/query/reading_clause/bound_match_clause.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ExistentialSubqueryExpression : public Expression {
public:
    ExistentialSubqueryExpression(
        std::unique_ptr<QueryGraphCollection> queryGraphCollection, const std::string& name)
        : Expression{common::EXISTENTIAL_SUBQUERY, common::BOOL, name},
          queryGraphCollection{std::move(queryGraphCollection)} {}

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(std::shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline std::shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    expression_vector getChildren() const override;

private:
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace kuzu
