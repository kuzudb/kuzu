#pragma once

#include "src/binder/include/bound_queries/bound_regular_query.h"
#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression_binder.h"
#include "src/binder/include/query_graph/query_graph.h"
#include "src/parser/include/queries/regular_query.h"
#include "src/parser/include/statements/match_statement.h"

using namespace graphflow::parser;

namespace graphflow {
namespace binder {

class QueryBinder {
    friend class ExpressionBinder;

public:
    explicit QueryBinder(const Catalog& catalog)
        : catalog{catalog}, lastExpressionId{0}, variablesInScope{}, expressionBinder{this} {}

    unique_ptr<BoundRegularQuery> bind(const RegularQuery& regularQuery);

private:
    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

    unique_ptr<BoundQueryPart> bindQueryPart(const QueryPart& queryPart);

    unique_ptr<BoundMatchStatement> bindMatchStatement(const MatchStatement& matchStatement);

    unique_ptr<BoundWithStatement> bindWithStatement(const WithStatement& withStatement);

    unique_ptr<BoundReturnStatement> bindReturnStatement(const ReturnStatement& returnStatement);

    unique_ptr<BoundProjectionBody> bindProjectionBody(
        const ProjectionBody& projectionBody, bool isWithClause);

    vector<shared_ptr<Expression>> bindProjectionExpressions(
        const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool isStar);

    vector<shared_ptr<Expression>> bindOrderByExpressions(
        const vector<unique_ptr<ParsedExpression>>& orderByExpressions,
        const unordered_map<string, shared_ptr<Expression>>& prevVariablesInScope,
        bool projectionHasAggregation);

    unordered_map<string, shared_ptr<Expression>> computeVariablesInScope(
        const vector<shared_ptr<Expression>>& expressions, bool isWithClause);

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
    void validateFirstMatchIsNotOptional(const SingleQuery& singleQuery);
    // E.g. MATCH (:person)-[:studyAt]->(:person)
    void validateNodeAndRelLabelIsConnected(
        label_t relLabel, label_t nodeLabel, Direction direction);
    // E.g. RETURN a, b AS a
    void validateProjectionColumnNamesAreUnique(const vector<shared_ptr<Expression>>& expressions);
    void validateOrderByFollowedBySkipOrLimitInWithStatement(
        const BoundProjectionBody& boundProjectionBody);
    void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
        unordered_map<string, shared_ptr<Expression>> prevVariablesInScope);
    uint64_t validateAndExtractSkipLimitNumber(const ParsedExpression& skipOrLimitExpression);

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
