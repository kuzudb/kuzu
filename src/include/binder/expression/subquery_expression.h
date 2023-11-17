#pragma once

#include "binder/query/query_graph.h"
#include "common/enums/subquery_type.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class SubqueryExpression : public Expression {
public:
    SubqueryExpression(common::SubqueryType subqueryType, common::LogicalType dataType,
        std::unique_ptr<QueryGraphCollection> queryGraphCollection, std::string uniqueName,
        std::string rawName)
        : Expression{common::ExpressionType::SUBQUERY, dataType, std::move(uniqueName)},
          subqueryType{subqueryType},
          queryGraphCollection{std::move(queryGraphCollection)}, rawName{std::move(rawName)} {}

    inline common::SubqueryType getSubqueryType() const { return subqueryType; }

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

    inline void setCountStarExpr(std::shared_ptr<Expression> expr) {
        countStarExpr = std::move(expr);
    }
    inline std::shared_ptr<Expression> getCountStarExpr() const { return countStarExpr; }
    inline void setProjectionExpr(std::shared_ptr<Expression> expr) {
        projectionExpr = std::move(expr);
    }
    inline std::shared_ptr<Expression> getProjectionExpr() const { return projectionExpr; }

    std::string toStringInternal() const final { return rawName; }

private:
    common::SubqueryType subqueryType;
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> whereExpression;
    std::shared_ptr<Expression> countStarExpr;
    std::shared_ptr<Expression> projectionExpr;
    std::string rawName;
};

} // namespace binder
} // namespace kuzu
