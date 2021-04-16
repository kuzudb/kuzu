#include "src/planner/include/variables_in_scope.h"

namespace graphflow {
namespace planner {

bool VariablesInScope::containsQueryNode(const string& name) const {
    return nameToQueryNodeMap.end() != nameToQueryNodeMap.find(name);
}

bool VariablesInScope::containsQueryRel(const string& name) const {
    return nameToQueryRelMap.end() != nameToQueryRelMap.find(name);
}

bool VariablesInScope::containsExpression(const string& name) const {
    return nameToExpressionMap.end() != nameToExpressionMap.find(name);
}

QueryNode* VariablesInScope::getQueryNode(const string& name) const {
    return nameToQueryNodeMap.at(name);
}

QueryRel* VariablesInScope::getQueryRel(const string& name) const {
    return nameToQueryRelMap.at(name);
}

shared_ptr<LogicalExpression> VariablesInScope::getExpression(const string& name) const {
    return nameToExpressionMap.at(name);
}

void VariablesInScope::addQueryNode(const string& name, QueryNode* queryNode) {
    nameToQueryNodeMap.insert({name, queryNode});
}

void VariablesInScope::addQueryRel(const string& name, QueryRel* queryRel) {
    nameToQueryRelMap.insert({name, queryRel});
}

void VariablesInScope::addExpression(const string& name, shared_ptr<LogicalExpression> expression) {
    nameToExpressionMap.insert({name, expression});
}

VariablesInScope VariablesInScope::copy() {
    auto mapCopy = VariablesInScope();
    mapCopy.nameToQueryNodeMap = nameToQueryNodeMap;
    mapCopy.nameToQueryRelMap = nameToQueryRelMap;
    mapCopy.nameToExpressionMap = nameToExpressionMap;
    return mapCopy;
}

} // namespace planner
} // namespace graphflow
