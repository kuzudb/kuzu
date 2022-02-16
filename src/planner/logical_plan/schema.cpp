#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

uint32_t Schema::createGroup() {
    auto pos = groups.size();
    groups.push_back(make_unique<FactorizationGroup>());
    return pos;
}

void Schema::insertToGroup(const string& expressionName, uint32_t groupPos) {
    expressionNameToGroupPos.insert({expressionName, groupPos});
    groups[groupPos]->insertExpression(expressionName);
}

void Schema::insertToGroupAndScope(const string& expressionName, uint32_t groupPos) {
    expressionNamesInScope.insert(expressionName);
    insertToGroup(expressionName, groupPos);
}

void Schema::insertToGroupAndScope(
    const unordered_set<string>& expressionNames, uint32_t groupPos) {
    for (auto& expressionName : expressionNames) {
        insertToGroupAndScope(expressionName, groupPos);
    }
}

uint32_t Schema::getGroupPos(const string& expressionName) const {
    assert(expressionNameToGroupPos.contains(expressionName));
    return expressionNameToGroupPos.at(expressionName);
}

unordered_set<string> Schema::getExpressionNamesInScope(uint32_t pos) const {
    unordered_set<string> result;
    for (auto& expressionName : groups[pos]->expressionNames) {
        if (expressionInScope(expressionName)) {
            result.insert(expressionName);
        }
    }
    return result;
}

void Schema::removeExpression(const string& expressionName) {
    auto groupPos = getGroupPos(expressionName);
    groups[groupPos]->removeExpression(expressionName);
    expressionNameToGroupPos.erase(expressionName);
}

unordered_set<uint32_t> Schema::getGroupsPosInScope() const {
    unordered_set<uint32_t> result;
    for (auto& expressionInScope : expressionNamesInScope) {
        result.insert(getGroupPos(expressionInScope));
    }
    return result;
}

void Schema::addLogicalExtend(const string& queryRel, LogicalExtend* extend) {
    queryRelLogicalExtendMap.insert({queryRel, extend});
}

LogicalExtend* Schema::getExistingLogicalExtend(const string& queryRel) {
    assert(queryRelLogicalExtendMap.contains(queryRel));
    return queryRelLogicalExtendMap.at(queryRel);
}

unique_ptr<Schema> Schema::copy() const {
    auto newSchema = make_unique<Schema>();
    newSchema->queryRelLogicalExtendMap = queryRelLogicalExtendMap;
    newSchema->expressionNameToGroupPos = expressionNameToGroupPos;
    for (auto& group : groups) {
        auto newGroup = make_unique<FactorizationGroup>(*group);
        newSchema->groups.push_back(move(newGroup));
    }
    newSchema->expressionNamesInScope = expressionNamesInScope;
    return newSchema;
}

void Schema::clear() {
    groups.clear();
    expressionNameToGroupPos.clear();
    clearExpressionsInScope();
}

} // namespace planner
} // namespace graphflow
