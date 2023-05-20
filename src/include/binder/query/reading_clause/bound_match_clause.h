#pragma once

#include "binder/expression/expression.h"
#include "bound_reading_clause.h"
#include "query_graph.h"

namespace kuzu {
namespace binder {

/**
 * BoundMatchClause may not have whereExpression
 */
class BoundMatchClause : public BoundReadingClause {

public:
    explicit BoundMatchClause(
        std::unique_ptr<QueryGraphCollection> queryGraphCollection, bool isOptional)
        : BoundReadingClause{common::ClauseType::MATCH},
          queryGraphCollection{std::move(queryGraphCollection)}, isOptional{isOptional} {}

    BoundMatchClause(const BoundMatchClause& other)
        : BoundReadingClause(common::ClauseType::MATCH),
          queryGraphCollection{other.queryGraphCollection->copy()},
          whereExpression(other.whereExpression), isOptional{other.isOptional} {}

    ~BoundMatchClause() override = default;

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(std::shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline std::shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    inline bool getIsOptional() const { return isOptional; }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundMatchClause>(*this);
    }

private:
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> whereExpression;
    bool isOptional;
};

} // namespace binder
} // namespace kuzu
