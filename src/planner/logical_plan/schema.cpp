#include "src/planner/include/logical_plan/schema.h"

#include <cassert>

namespace graphflow {
namespace planner {

bool FactorizationGroup::containsVariable(const string& variable) const {
    return variables.contains(variable);
}

void FactorizationGroup::addVariables(unordered_set<string> variablesToAdd) {
    variables.insert(variablesToAdd.begin(), variablesToAdd.end());
}

void Schema::addMatchedAttribute(const string& attribute) {
    matchedAttributes.insert(attribute);
}

void Schema::addQueryRelAndLogicalExtend(const string& queryRel, LogicalExtend* extend) {
    queryRelLogicalExtendMap.insert({queryRel, extend});
}

bool Schema::containsAttributeName(const string& attribute) const {
    return matchedAttributes.contains(attribute);
}

LogicalExtend* Schema::getExistingLogicalExtend(const string& queryRel) const {
    return queryRelLogicalExtendMap.at(queryRel);
}

bool Schema::isVariableFlat(const string& variable) const {
    return flatGroup->containsVariable(variable);
}

FactorizationGroup* Schema::getFactorizationGroup(const string& variable) {
    if (flatGroup->containsVariable(variable)) {
        return flatGroup.get();
    }
    return unflatGroups[variableUnflatGroupPosMap.at(variable)].get();
}

void Schema::initFlatFactorizationGroup(const string& variable, uint64_t cardinality) {
    flatGroup = make_unique<FactorizationGroup>(unordered_set<string>{variable}, cardinality);
}

void Schema::addUnFlatFactorizationGroup(unordered_set<string> variables, uint64_t extensionRate) {
    for (auto& variable : variables) {
        variableUnflatGroupPosMap.insert({variable, unflatGroups.size()});
    }
    unflatGroups.emplace_back(make_unique<FactorizationGroup>(move(variables), extensionRate));
}

void Schema::flattenFactorizationGroupIfNecessary(const string& variable) {
    if (flatGroup->containsVariable(variable)) {
        return;
    }
    flattenFactorizationGroup(*unflatGroups[variableUnflatGroupPosMap.at(variable)]);
}

uint64_t Schema::getCardinality() const {
    return flatGroup->cardinalityOrExtensionRate;
}

void Schema::merge(Schema& other) {
    matchedAttributes.insert(other.matchedAttributes.begin(), other.matchedAttributes.end());
    queryRelLogicalExtendMap.insert(
        other.queryRelLogicalExtendMap.begin(), other.queryRelLogicalExtendMap.end());
}

unique_ptr<Schema> Schema::copy() {
    auto schema = make_unique<Schema>();
    schema->matchedAttributes = matchedAttributes;
    schema->queryRelLogicalExtendMap = queryRelLogicalExtendMap;
    schema->flatGroup = make_unique<FactorizationGroup>(*flatGroup);
    schema->variableUnflatGroupPosMap = variableUnflatGroupPosMap;
    for (auto& unflatGroup : unflatGroups) {
        schema->unflatGroups.emplace_back(make_unique<FactorizationGroup>(*unflatGroup));
    }
    return schema;
}

void Schema::flattenFactorizationGroup(const FactorizationGroup& groupToFlatten) {
    assert(!flatGroup->variables.empty());
    for (auto& variable : groupToFlatten.variables) {
        variableUnflatGroupPosMap.erase(variable);
    }
    flatGroup->cardinalityOrExtensionRate *= groupToFlatten.cardinalityOrExtensionRate;
    flatGroup->addVariables(groupToFlatten.variables);
}

} // namespace planner
} // namespace graphflow
