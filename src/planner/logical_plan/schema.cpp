#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

uint32_t Schema::createGroup() {
    auto pos = groups.size();
    groups.push_back(make_unique<FactorizationGroup>());
    return pos;
}

void Schema::insertToGroup(const string& variable, uint32_t groupPos) {
    variableToGroupPos.insert({variable, groupPos});
    groups[groupPos]->insertVariable(variable);
}

void Schema::insertToGroup(const FactorizationGroup& otherGroup, uint32_t groupPos) {
    for (auto& variable : otherGroup.variables) {
        insertToGroup(variable, groupPos);
    }
}

void Schema::flattenGroup(uint32_t pos) {
    groups[pos]->isFlat = true;
}

uint32_t Schema::getGroupPos(const string& variable) const {
    GF_ASSERT(variableToGroupPos.contains(variable));
    return variableToGroupPos.at(variable);
}

unordered_set<uint32_t> Schema::getUnFlatGroupsPos() const {
    unordered_set<uint32_t> result;
    for (auto i = 0u; i < groups.size(); ++i) {
        if (!groups[i]->isFlat) {
            result.insert(i);
        }
    }
    return result;
}

uint32_t Schema::getAnyGroupPos() const {
    GF_ASSERT(!groups.empty());
    return 0;
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
    newSchema->variableToGroupPos = variableToGroupPos;
    for (auto& group : groups) {
        auto newGroup = make_unique<FactorizationGroup>(*group);
        newSchema->groups.push_back(move(newGroup));
    }
    return newSchema;
}

void Schema::clearGroups() {
    groups.clear();
    variableToGroupPos.clear();
}

} // namespace planner
} // namespace graphflow
