#pragma once

#include "bound_reading_clause.h"
#include "query_graph.h"

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace binder {

/**
 * BoundMatchClause may not have whereExpression
 */
class BoundMatchClause : public BoundReadingClause {

public:
    explicit BoundMatchClause(unique_ptr<QueryGraph> queryGraph, bool isOptional)
        : BoundReadingClause{ClauseType::MATCH}, queryGraph{move(queryGraph)}, isOptional{
                                                                                   isOptional} {}

    BoundMatchClause(const BoundMatchClause& other)
        : BoundReadingClause(ClauseType::MATCH), queryGraph{make_unique<QueryGraph>(
                                                     *other.queryGraph)},
          whereExpression(other.whereExpression), isOptional{other.isOptional} {}

    ~BoundMatchClause() = default;

    inline QueryGraph* getQueryGraph() const { return queryGraph.get(); }

    inline void setWhereExpression(shared_ptr<Expression> expression) {
        whereExpression = move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    inline bool getIsOptional() const { return isOptional; }

    inline unique_ptr<BoundReadingClause> copy() override {
        return make_unique<BoundMatchClause>(*this);
    }

private:
    unique_ptr<QueryGraph> queryGraph;
    shared_ptr<Expression> whereExpression;
    bool isOptional;
};

} // namespace binder
} // namespace graphflow
