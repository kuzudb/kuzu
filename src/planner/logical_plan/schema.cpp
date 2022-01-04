#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

uint32_t Schema::createGroup() {
    auto pos = groups.size();
    groups.push_back(make_unique<FactorizationGroup>());
    return pos;
}

void Schema::insertToGroup(const string& expressionName, uint32_t groupPos) {
    auto k = 1;
    expressionNameToGroupPos.insert({expressionName, groupPos});
    groups[groupPos]->insertExpression(expressionName);
}

void Schema::insertToGroup(const FactorizationGroup& otherGroup, uint32_t groupPos) {
    for (auto& expressionName : otherGroup.expressionNames) {
        insertToGroup(expressionName, groupPos);
    }
}

void Schema::flattenGroup(uint32_t pos) {
    groups[pos]->isFlat = true;
}

uint32_t Schema::getGroupPos(const string& expressionName) const {
    GF_ASSERT(expressionNameToGroupPos.contains(expressionName));
    return expressionNameToGroupPos.at(expressionName);
}

unordered_set<uint32_t> Schema::getGroupsPos() const {
    unordered_set<uint32_t> groupsPos;
    for (auto i = 0u; i < groups.size(); ++i) {
        groupsPos.insert(i);
    }
    return groupsPos;
}

void Schema::addLogicalExtend(const string& queryRel, LogicalExtend* extend) {
    queryRelLogicalExtendMap.insert({queryRel, extend});
}

LogicalExtend* Schema::getExistingLogicalExtend(const string& queryRel) {
    GF_ASSERT(queryRelLogicalExtendMap.contains(queryRel));
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
    return newSchema;
}

void Schema::clearGroups() {
    groups.clear();
    expressionNameToGroupPos.clear();
}

} // namespace planner
} // namespace graphflow
