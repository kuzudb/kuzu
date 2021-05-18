#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/expression_binder.h"
#include "src/parser/include/queries/single_query.h"
#include "src/parser/include/statements/match_statement.h"

namespace graphflow {
namespace binder {

class QueryBinder {

public:
    explicit QueryBinder(const Catalog& catalog) : catalog{catalog}, lastVariableIdx{0} {}

    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

private:
    unique_ptr<BoundMatchStatement> bindMatchStatementsAndMerge(
        const vector<unique_ptr<ReadingStatement>>& readingStatements);

    unique_ptr<BoundMatchStatement> bindMatchStatement(const MatchStatement& matchStatement);

    unique_ptr<BoundWithStatement> bindWithStatement(const WithStatement& withStatement);

    unique_ptr<BoundReturnStatement> bindReturnStatement(const ReturnStatement& returnStatement);

    shared_ptr<Expression> bindWhereExpression(const ParsedExpression& parsedExpression);

    vector<shared_ptr<Expression>> bindProjectExpressions(
        const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar);

    unique_ptr<QueryGraph> bindQueryGraph(const vector<unique_ptr<PatternElement>>& graphPattern);

    void bindQueryRel(const RelPattern& relPattern, NodeExpression* leftNode,
        NodeExpression* rightNode, QueryGraph& queryGraph);

    void bindNodeToRel(RelExpression& queryRel, NodeExpression* queryNode, bool isSrcNode);

    shared_ptr<NodeExpression> bindQueryNode(
        const NodePattern& nodePattern, QueryGraph& queryGraph);

    label_t bindRelLabel(const string& parsed_label);

    label_t bindNodeLabel(const string& parsed_label);

private:
    const Catalog& catalog;
    unordered_map<string, shared_ptr<Expression>> variablesInScope;
    uint32_t lastVariableIdx;
};

} // namespace binder
} // namespace graphflow
