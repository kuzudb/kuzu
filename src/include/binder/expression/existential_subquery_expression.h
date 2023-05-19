#pragma once

#include "binder/query/reading_clause/bound_match_clause.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ExistentialSubqueryExpression : public Expression {
public:
    ExistentialSubqueryExpression(std::unique_ptr<QueryGraphCollection> queryGraphCollection,
        std::string uniqueName, std::string rawName)
        : Expression{common::EXISTENTIAL_SUBQUERY, common::LogicalType(common::LogicalTypeID::BOOL),
              std::move(uniqueName)},
          queryGraphCollection{std::move(queryGraphCollection)}, rawName{std::move(rawName)} {}

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(std::shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline std::shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    expression_vector getChildren() const override;

    std::string toString() const override { return rawName; }

private:
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> whereExpression;
    std::string rawName;
};

} // namespace binder
} // namespace kuzu
