#pragma once

#include <unordered_map>
#include <unordered_set>

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"

namespace graphflow {
namespace planner {

// Represents a group of variables that are logically factorized together
struct FactorizationGroup {

public:
    FactorizationGroup(unordered_set<string> variables, uint32_t cardinalityOrExtensionRate)
        : variables{move(variables)}, cardinalityOrExtensionRate{cardinalityOrExtensionRate} {}

    FactorizationGroup(const FactorizationGroup& factorizationGroup)
        : variables{factorizationGroup.variables},
          cardinalityOrExtensionRate{factorizationGroup.cardinalityOrExtensionRate} {}

    bool containsVariable(const string& variable) const;

    void addVariables(unordered_set<string> variablesToAdd);

public:
    unordered_set<string> variables;
    // For flat factorization group, we store cardinality of the sub-query that contains variables
    // in flat factorization group. Otherwise, for unflat factorization group, we store the
    // extension rate per flat prefix.
    uint64_t cardinalityOrExtensionRate;
};

class Schema {

public:
    void addMatchedAttribute(const string& attribute);

    void addQueryRelAndLogicalExtend(const string& queryRel, LogicalExtend* extend);

    bool containsAttributeName(const string& attribute) const;

    // Returns the LogicalExtend that matches the queryRel
    LogicalExtend* getExistingLogicalExtend(const string& queryRel) const;

    bool isVariableFlat(const string& variable) const;

    FactorizationGroup* getFactorizationGroup(const string& variable);

    void initFlatFactorizationGroup(const string& variable, uint64_t cardinality);

    void addUnFlatFactorizationGroup(unordered_set<string> variables, uint64_t extensionRate);

    void flattenFactorizationGroupIfNecessary(const string& variable);

    uint64_t getCardinality() const;

    void merge(Schema& other);

    unique_ptr<Schema> copy();

private:
    void flattenFactorizationGroup(const FactorizationGroup& groupToFlatten);

public:
    unordered_set<string> matchedAttributes;
    // Maps a queryRel to the LogicalExtend that matches it. This is needed because ScanRelProperty
    // requires direction information which only available in the LogicalExtend.
    unordered_map<string, LogicalExtend*> queryRelLogicalExtendMap;
    // All flat variables are considered as in the same factorization group
    unique_ptr<FactorizationGroup> flatGroup;
    unordered_map<string, uint32_t> variableUnflatGroupPosMap;
    vector<unique_ptr<FactorizationGroup>> unflatGroups;
};

} // namespace planner
} // namespace graphflow
