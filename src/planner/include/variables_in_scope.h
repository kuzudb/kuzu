#pragma once

#include "src/expression/include/logical/logical_expression.h"
#include "src/planner/include/query_graph/query_rel.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class VariablesInScope {

public:
    bool containsQueryNode(const string& name) const;

    bool containsQueryRel(const string& name) const;

    bool containsExpression(const string& name) const;

    shared_ptr<QueryNode> getQueryNode(const string& name) const;

    shared_ptr<QueryRel> getQueryRel(const string& name) const;

    shared_ptr<LogicalExpression> getExpression(const string& name) const;

    void addQueryNode(const string& name, shared_ptr<QueryNode> queryNode);

    void addQueryRel(const string& name, shared_ptr<QueryRel> queryRel);

    void addExpression(const string& name, shared_ptr<LogicalExpression> expression);

    VariablesInScope copy();

public:
    unordered_map<string, shared_ptr<QueryNode>> nameToQueryNodeMap;
    unordered_map<string, shared_ptr<QueryRel>> nameToQueryRelMap;
    unordered_map<string, shared_ptr<LogicalExpression>> nameToExpressionMap;
};

} // namespace planner
} // namespace graphflow
