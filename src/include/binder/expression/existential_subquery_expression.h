#pragma once

#include "binder/query/query_graph.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ExistentialSubqueryExpression : public Expression {
public:
    ExistentialSubqueryExpression(std::unique_ptr<QueryGraphCollection> queryGraphCollection,
        std::string uniqueName, std::string rawName)
        : Expression{common::ExpressionType::EXISTENTIAL_SUBQUERY,
              common::LogicalType(common::LogicalTypeID::BOOL), std::move(uniqueName)},
          queryGraphCollection{std::move(queryGraphCollection)}, rawName{std::move(rawName)} {}

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(std::shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline std::shared_ptr<Expression> getWhereExpression() const { return whereExpression; }
    inline expression_vector getPredicatesSplitOnAnd() const {
        return hasWhereExpression() ? whereExpression->splitOnAND() : expression_vector{};
    }

    std::string toStringInternal() const final { return rawName; }

private:
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> whereExpression;
    std::string rawName;
};

} // namespace binder
} // namespace kuzu
