#include "src/planner/include/logical_plan/schema.h"

#include "src/common/include/assert.h"

namespace graphflow {
namespace planner {

uint32_t Schema::createGroup() {
    auto pos = groups.size();
    groups.emplace_back(FactorizationGroup());
    return pos;
}

void Schema::appendToGroup(const string& variable, uint32_t pos) {
    variableToGroupPos.insert({variable, pos});
    groups[pos].appendVariable(variable);
}

void Schema::appendToGroup(FactorizationGroup& otherGroup, uint32_t pos) {
    for (auto& variable : otherGroup.variables) {
        appendToGroup(variable, pos);
    }
}

void Schema::flattenGroup(uint32_t pos) {
    groups[pos].isFlat = true;
}

uint32_t Schema::getGroupPos(const string& variable) {
    GF_ASSERT(variableToGroupPos.contains(variable));
    return variableToGroupPos.at(variable);
}

void Schema::addLogicalExtend(const string& queryRel, LogicalExtend* extend) {
    queryRelLogicalExtendMap.insert({queryRel, extend});
}

LogicalExtend* Schema::getExistingLogicalExtend(const string& queryRel) {
    GF_ASSERT(queryRelLogicalExtendMap.contains(queryRel));
    return queryRelLogicalExtendMap.at(queryRel);
}

unique_ptr<Schema> Schema::copy() {
    auto newSchema = make_unique<Schema>();
    newSchema->queryRelLogicalExtendMap = queryRelLogicalExtendMap;
    newSchema->variableToGroupPos = variableToGroupPos;
    newSchema->groups = groups;
    return newSchema;
}

} // namespace planner
} // namespace graphflow
