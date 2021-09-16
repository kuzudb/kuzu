#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression_binder.h"
#include "src/binder/include/query_graph/query_graph.h"
#include "src/parser/include/queries/single_query.h"
#include "src/parser/include/statements/load_csv_statement.h"
#include "src/parser/include/statements/match_statement.h"

using namespace graphflow::parser;

namespace graphflow {
namespace binder {

class QueryBinder {
    friend class ExpressionBinder;

public:
    explicit QueryBinder(const Catalog& catalog)
        : catalog{catalog}, lastVariableId{0}, variablesInScope{}, expressionBinder{this} {}

    unique_ptr<BoundSingleQuery> bind(const SingleQuery& singleQuery);

private:
    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

    unique_ptr<BoundQueryPart> bindQueryPart(const QueryPart& queryPart);

    unique_ptr<BoundReadingStatement> bindReadingStatement(
        const ReadingStatement& readingStatement);

    unique_ptr<BoundReadingStatement> bindLoadCSVStatement(
        const LoadCSVStatement& loadCSVStatement);

    unique_ptr<BoundReadingStatement> bindMatchStatement(const MatchStatement& matchStatement);

    unique_ptr<BoundWithStatement> bindWithStatement(const WithStatement& withStatement);

    unique_ptr<BoundReturnStatement> bindReturnStatement(const ReturnStatement& returnStatement);

    unique_ptr<BoundProjectionBody> bindProjectionBody(
        const ProjectionBody& projectionBody, bool updateVariablesInScope);

    shared_ptr<Expression> bindWhereExpression(const ParsedExpression& parsedExpression);

    vector<shared_ptr<Expression>> bindProjectExpressions(
        const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar,
        bool updateVariablesInScope);

    unique_ptr<QueryGraph> bindQueryGraph(const vector<unique_ptr<PatternElement>>& graphPattern);

    void bindQueryRel(const RelPattern& relPattern, const shared_ptr<NodeExpression>& leftNode,
        const shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph);

    shared_ptr<NodeExpression> bindQueryNode(
        const NodePattern& nodePattern, QueryGraph& queryGraph);

    label_t bindRelLabel(const string& parsed_label);

    label_t bindNodeLabel(const string& parsed_label);

    /******* validations *********/

    // E.g. MATCH (:person)-[:studyAt]->(:person)
    void validateNodeAndRelLabelIsConnected(
        label_t relLabel, label_t nodeLabel, Direction direction);
    // E.g. RETURN a, b AS a
    void validateProjectionColumnNamesAreUnique(const vector<shared_ptr<Expression>>& expressions);
    // Current system does not support aggregations with group by.
    void validateAggregationsHaveNoGroupBy(const vector<shared_ptr<Expression>>& expressions);
    void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
        unordered_map<string, shared_ptr<Expression>> prevVariablesInScope);
    void validateCSVHeaderColumnNamesAreUnique(const vector<pair<string, DataType>>& headerInfo);
    uint64_t validateAndExtractSkipLimitNumber(const ParsedExpression& skipOrLimitExpression);

    /******* helpers *********/
    string getUniqueVariableName(const string& name);

    unordered_map<string, shared_ptr<Expression>> enterSubquery();
    void exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope);

private:
    const Catalog& catalog;
    uint32_t lastVariableId;
    unordered_map<string, shared_ptr<Expression>> variablesInScope;
    ExpressionBinder expressionBinder;
};

} // namespace binder
} // namespace graphflow
