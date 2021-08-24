#pragma once

#include "src/binder/include/bound_statements/bound_reading_statement.h"
#include "src/binder/include/expression/expression.h"
#include "src/binder/include/query_graph/query_graph.h"

namespace graphflow {
namespace binder {

/**
 * BoundMatchStatement may not have whereExpression
 */
class BoundMatchStatement : public BoundReadingStatement {

public:
    explicit BoundMatchStatement(unique_ptr<QueryGraph> queryGraph)
        : BoundReadingStatement(MATCH_STATEMENT), queryGraph{move(queryGraph)} {}

    void merge(BoundMatchStatement& other) {
        queryGraph->merge(*other.queryGraph);
        if (other.whereExpression) {
            whereExpression = whereExpression ? make_shared<Expression>(AND, BOOL, whereExpression,
                                                    other.whereExpression) :
                                                other.whereExpression;
        }
    }

    vector<shared_ptr<Expression>> getIncludedVariables() const override {
        vector<shared_ptr<Expression>> result;
        for (auto& queryNode : queryGraph->queryNodes) {
            result.push_back(queryNode);
        }
        for (auto& queryRel : queryGraph->queryRels) {
            result.push_back(queryRel);
        }
        if (whereExpression) {
            for (auto& expression : whereExpression->getIncludedVariableExpressions()) {
                result.push_back(expression);
            }
        }
        return result;
    }

public:
    unique_ptr<QueryGraph> queryGraph;
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace graphflow
