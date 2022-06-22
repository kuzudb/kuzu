#pragma once

#include "expression_binder.h"
#include "query_normalizer.h"

#include "src/binder/query/include/bound_regular_query.h"
#include "src/parser/query/include/regular_query.h"

using namespace graphflow::parser;
using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class QueryBinder {
    friend class ExpressionBinder;

public:
    explicit QueryBinder(const Catalog& catalog)
        : catalog{catalog}, lastExpressionId{0}, variablesInScope{}, expressionBinder{this} {}

    unique_ptr<BoundRegularQuery> bind(const RegularQuery& regularQuery);

    inline unordered_map<string, shared_ptr<Literal>> getParameterMap() {
        return expressionBinder.parameterMap;
    }

private:
    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

    unique_ptr<BoundQueryPart> bindQueryPart(const QueryPart& queryPart);

    unique_ptr<BoundMatchClause> bindMatchClause(const MatchClause& matchClause);

    unique_ptr<BoundUpdatingClause> bindUpdatingClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindSetClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindDeleteClause(const UpdatingClause& updatingClause);

    unique_ptr<BoundWithClause> bindWithClause(const WithClause& withClause);

    unique_ptr<BoundReturnClause> bindReturnClause(const ReturnClause& returnClause);

    expression_vector bindProjectionExpressions(
        const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool containsStar);

    // For RETURN clause, we write variable "v" as all properties of "v"
    expression_vector rewriteProjectionExpressions(const expression_vector& expressions);

    expression_vector rewriteNodeAsAllProperties(const shared_ptr<Expression>& expression);

    expression_vector rewriteRelAsAllProperties(const shared_ptr<Expression>& expression);

    void bindOrderBySkipLimitIfNecessary(
        BoundProjectionBody& boundProjectionBody, const ProjectionBody& projectionBody);

    expression_vector bindOrderByExpressions(
        const vector<unique_ptr<ParsedExpression>>& orderByExpressions);

    uint64_t bindSkipLimitExpression(const ParsedExpression& expression);

    void addExpressionsToScope(const expression_vector& projectionExpressions);

    shared_ptr<Expression> bindWhereExpression(const ParsedExpression& parsedExpression);

    unique_ptr<QueryGraph> bindQueryGraph(const vector<unique_ptr<PatternElement>>& graphPattern);

    void bindQueryRel(const RelPattern& relPattern, const shared_ptr<NodeExpression>& leftNode,
        const shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph);

    shared_ptr<NodeExpression> bindQueryNode(
        const NodePattern& nodePattern, QueryGraph& queryGraph);

    label_t bindRelLabel(const string& parsed_label);

    label_t bindNodeLabel(const string& parsed_label);

    /******* validations *********/
    // E.g. Optional MATCH (a) RETURN a.age
    // Although this is doable in Neo4j, I don't think the semantic make a lot of sense because
    // there is nothing to left join on.
    static void validateFirstMatchIsNotOptional(const SingleQuery& singleQuery);

    // E.g. MATCH (:person)-[:studyAt]->(:person) ...
    static void validateNodeAndRelLabelIsConnected(
        const Catalog& catalog_, label_t relLabel, label_t nodeLabel, RelDirection direction);

    // E.g. ... RETURN a, b AS a
    static void validateProjectionColumnNamesAreUnique(const expression_vector& expressions);

    // E.g. ... WITH COUNT(*) MATCH ...
    static void validateProjectionColumnsInWithClauseAreAliased(
        const expression_vector& expressions);

    static void validateOrderByFollowedBySkipOrLimitInWithClause(
        const BoundProjectionBody& boundProjectionBody);

    static void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
        const unordered_map<string, shared_ptr<Expression>>& prevVariablesInScope);

    static void validateUnionColumnsOfTheSameType(
        const vector<unique_ptr<BoundSingleQuery>>& boundSingleQueries);

    static void validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery);

    static void validateReadNotFollowUpdate(const NormalizedSingleQuery& normalizedSingleQuery);

    /******* helpers *********/

    string getUniqueExpressionName(const string& name);

    unordered_map<string, shared_ptr<Expression>> enterSubquery();
    void exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope);

private:
    const Catalog& catalog;
    uint32_t lastExpressionId;
    unordered_map<string, shared_ptr<Expression>> variablesInScope;
    ExpressionBinder expressionBinder;
};

} // namespace binder
} // namespace graphflow
