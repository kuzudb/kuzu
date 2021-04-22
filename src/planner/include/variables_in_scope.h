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

    QueryNode* getQueryNode(const string& name) const;

    QueryRel* getQueryRel(const string& name) const;

    shared_ptr<LogicalExpression> getExpression(const string& name) const;

    void addQueryNode(const string& name, QueryNode* queryNode);

    void addQueryRel(const string& name, QueryRel* queryRel);

    void addExpression(const string& name, shared_ptr<LogicalExpression> expression);

    VariablesInScope copy();

public:
    unordered_map<string, QueryNode*> nameToQueryNodeMap;
    unordered_map<string, QueryRel*> nameToQueryRelMap;
    unordered_map<string, shared_ptr<LogicalExpression>> nameToExpressionMap;
};

} // namespace planner
} // namespace graphflow
