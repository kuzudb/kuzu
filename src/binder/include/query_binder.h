#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression_binder.h"
#include "src/binder/include/query_graph/query_graph.h"
#include "src/parser/include/queries/single_query.h"
#include "src/parser/include/statements/load_csv_statement.h"
#include "src/parser/include/statements/match_statement.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::parser;
using namespace graphflow::storage;

namespace graphflow {
namespace binder {

class QueryBinder {

public:
    explicit QueryBinder(const Catalog& catalog)
        : catalog{catalog}, variablesInScope{unordered_map<string, shared_ptr<Expression>>()},
          lastVariableId{0}, expressionBinder{catalog, variablesInScope} {}

    unique_ptr<BoundSingleQuery> bind(const SingleQuery& singleQuery);

private:
    /* optimizing functions */
    void optimizeReadingStatements(BoundSingleQuery& boundSingleQuery);

    /* binding functions */
    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

    unique_ptr<BoundQueryPart> bindQueryPart(const QueryPart& queryPart);

    unique_ptr<BoundReadingStatement> bindReadingStatement(
        const ReadingStatement& readingStatement);

    unique_ptr<BoundReadingStatement> bindLoadCSVStatement(
        const LoadCSVStatement& loadCSVStatement);

    unique_ptr<BoundReadingStatement> bindMatchStatement(const MatchStatement& matchStatement);

    unique_ptr<BoundWithStatement> bindWithStatement(const WithStatement& withStatement);

    unique_ptr<BoundReturnStatement> bindReturnStatement(const ReturnStatement& returnStatement);

    shared_ptr<Expression> bindWhereExpression(const ParsedExpression& parsedExpression);

    vector<shared_ptr<Expression>> bindProjectExpressions(
        const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar,
        bool updateVariablesInScope);

    unique_ptr<QueryGraph> bindQueryGraph(const vector<unique_ptr<PatternElement>>& graphPattern);

    void bindQueryRel(const RelPattern& relPattern, NodeExpression* leftNode,
        NodeExpression* rightNode, QueryGraph& queryGraph);

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
    // Current system does not support group by and aggregation. So if COUNT(*) presents, no other
    // expression is allowed
    void validateOnlyFunctionIsCountStar(const vector<shared_ptr<Expression>>& expressions);
    void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
        unordered_map<string, shared_ptr<Expression>> prevVariablesInScope);
    void validateCSVHeaderColumnNamesAreUnique(const vector<pair<string, DataType>>& headerInfo);

private:
    const Catalog& catalog;
    unordered_map<string, shared_ptr<Expression>> variablesInScope;
    uint32_t lastVariableId;
    ExpressionBinder expressionBinder;
};

} // namespace binder
} // namespace graphflow
