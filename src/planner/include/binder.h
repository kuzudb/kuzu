#pragma once

#include "src/parser/include/queries/single_query.h"
#include "src/planner/include/bound_statements/bound_match_statement.h"
#include "src/planner/include/bound_statements/bound_single_query.h"
#include "src/planner/include/expression_binder.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::storage;
using namespace graphflow::parser;

namespace graphflow {
namespace planner {

class Binder {

public:
    explicit Binder(const Catalog& catalog) : catalog{catalog} {}

    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

private:
    unique_ptr<BoundMatchStatement> bindMatchStatement(const MatchStatement& matchStatement);

    unique_ptr<BoundReturnStatement> bindReturnStatement(
        ReturnStatement& returnStatement, const QueryGraph& graphInScope);

    unique_ptr<QueryGraph> bindQueryGraph(const vector<unique_ptr<PatternElement>>& graphPattern);

    QueryRel* bindQueryRel(const PatternElementChain& patternElementChain, QueryNode* leftNode,
        QueryGraph& queryGraph);

    QueryNode* bindQueryNode(const NodePattern& nodePattern, QueryGraph& queryGraph);

    label_t bindRelLabel(const string& parsed_label);

    label_t bindNodeLabel(const string& parsed_label);

    // set queryNode as src or dst for queryRel
    void bindNodeToRel(QueryRel* queryRel, QueryNode* queryNode, bool isSrcNode);

private:
    const Catalog& catalog;
};

} // namespace planner
} // namespace graphflow
