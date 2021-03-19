#pragma once

#include "src/parser/include/single_query.h"
#include "src/planner/include/bound_statements/bound_match_statement.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::storage;
using namespace graphflow::parser;

namespace graphflow {
namespace planner {

class Binder {

public:
    explicit Binder(const Catalog& catalog) : catalog{catalog} {}

    vector<unique_ptr<BoundMatchStatement>> bindSingleQuery(const SingleQuery& singleQuery);

private:
    unique_ptr<BoundMatchStatement> bindStatement(const MatchStatement& matchStatement);

    void bindQueryRels(const PatternElement& patternElement, QueryGraph& queryGraph);

    QueryRel* bindQueryRel(const PatternElementChain& patternElementChain, QueryNode* leftNode,
        QueryGraph& queryGraph);

    QueryNode* bindQueryNode(const NodePattern& nodePattern, QueryGraph& queryGraph);

    int bindRelLabel(const string& parsed_label);

    int bindNodeLabel(const string& parsed_label);

    void bindNodeToRel(QueryRel* queryRel, QueryNode* queryNode, bool isSrcNode);

private:
    const Catalog& catalog;
};

} // namespace planner
} // namespace graphflow
