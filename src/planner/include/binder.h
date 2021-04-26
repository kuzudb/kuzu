#pragma once

#include "src/parser/include/queries/single_query.h"
#include "src/planner/include/bound_queries/bound_single_query.h"
#include "src/planner/include/expression_binder.h"

namespace graphflow {
namespace planner {

class Binder {

public:
    explicit Binder(const Catalog& catalog) : catalog{catalog}, lastVariableIdx{0} {}

    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

private:
    unique_ptr<BoundMatchStatement> bindMatchStatementsAndMerge(
        const vector<unique_ptr<MatchStatement>>& matchStatements);

    unique_ptr<BoundMatchStatement> bindMatchStatement(const MatchStatement& matchStatement);

    unique_ptr<BoundWithStatement> bindWithStatement(const WithStatement& withStatement);

    unique_ptr<BoundReturnStatement> bindReturnStatement(const ReturnStatement& returnStatement);

    shared_ptr<LogicalExpression> bindWhereExpression(const ParsedExpression& parsedExpression);

    vector<shared_ptr<LogicalExpression>> bindProjectExpressions(
        const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar);

    unique_ptr<QueryGraph> bindQueryGraph(const vector<unique_ptr<PatternElement>>& graphPattern);

    void bindQueryRel(const RelPattern& relPattern, LogicalNodeExpression* leftNode,
        LogicalNodeExpression* rightNode, QueryGraph& queryGraph);

    void bindNodeToRel(
        LogicalRelExpression& queryRel, LogicalNodeExpression* queryNode, bool isSrcNode);

    shared_ptr<LogicalNodeExpression> bindQueryNode(
        const NodePattern& nodePattern, QueryGraph& queryGraph);

    label_t bindRelLabel(const string& parsed_label);

    label_t bindNodeLabel(const string& parsed_label);

private:
    const Catalog& catalog;
    unordered_map<string, shared_ptr<LogicalExpression>> variablesInScope;
    uint32_t lastVariableIdx;
};

} // namespace planner
} // namespace graphflow
