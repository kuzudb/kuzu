#pragma once

#include "src/binder/include/bound_statements/bound_load_csv_statement.h"
#include "src/binder/include/bound_statements/bound_projection_body.h"
#include "src/binder/include/query_graph/query_graph.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

/**
 * NormalizedQueryPart
 * may have any combination of the following
 *      loadCSVStatement
 *      queryGraph
 *      whereExpression (If both MATCH and WITH has where clause,
 *          whereExpression = matchWhere AND withWhere)
 *
 * must have
 *      projectionBody
 */
class NormalizedQueryPart {

public:
    NormalizedQueryPart(unique_ptr<BoundProjectionBody> projectionBody, bool lastQueryPart)
        : queryGraph{make_unique<QueryGraph>()}, projectionBody{move(projectionBody)},
          lastQueryPart{lastQueryPart} {}

    inline bool isLastQueryPart() const { return lastQueryPart; }

    inline bool hasLoadCSVStatement() const { return loadCSVStatement != nullptr; }
    inline BoundLoadCSVStatement* getLoadCSVStatement() const { return loadCSVStatement.get(); }
    inline void setLoadCSVStatement(unique_ptr<BoundLoadCSVStatement> statement) {
        loadCSVStatement = move(statement);
    }

    inline bool hasQueryGraph() const { return !queryGraph->isEmpty(); }
    inline QueryGraph* getQueryGraph() const { return queryGraph.get(); }
    inline void addQueryGraph(const QueryGraph& otherGraph) { queryGraph->merge(otherGraph); }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }
    inline void addWhereExpression(const shared_ptr<Expression>& expression) {
        whereExpression = hasWhereExpression() ?
                              make_shared<Expression>(AND, BOOL, whereExpression, expression) :
                              expression;
    }

    inline BoundProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    vector<shared_ptr<Expression>> getDependentNodeID() const;
    vector<shared_ptr<Expression>> getDependentPropertiesFromWhereAndProjection() const;

private:
    unique_ptr<BoundLoadCSVStatement> loadCSVStatement;
    unique_ptr<QueryGraph> queryGraph;
    shared_ptr<Expression> whereExpression;
    unique_ptr<BoundProjectionBody> projectionBody;

    bool lastQueryPart;
};

} // namespace planner
} // namespace graphflow
