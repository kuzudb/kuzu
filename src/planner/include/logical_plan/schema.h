#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/common/include/assert.h"

using namespace std;

namespace graphflow {
namespace planner {

class LogicalExtend;

class FactorizationGroup {

public:
    FactorizationGroup() : isFlat{false}, estimatedCardinality{1} {}

    FactorizationGroup(const FactorizationGroup& other)
        : isFlat{other.isFlat},
          estimatedCardinality{other.estimatedCardinality}, variables{other.variables} {}

    void appendVariable(const string& variable) { variables.push_back(variable); }

    inline string getAnyVariable() {
        GF_ASSERT(!variables.empty());
        return variables[0];
    }

public:
    bool isFlat;
    uint64_t estimatedCardinality;
    vector<string> variables;
};

class Schema {

public:
    uint32_t createGroup();

    void appendToGroup(const string& variable, uint32_t pos);

    void appendToGroup(const FactorizationGroup& otherGroup, uint32_t pos);

    uint32_t getGroupPos(const string& variable) const;

    void flattenGroup(uint32_t pos);

    bool containVariable(const string& variable) const {
        return variableToGroupPos.contains(variable);
    }

    void addLogicalExtend(const string& queryRel, LogicalExtend* extend);

    LogicalExtend* getExistingLogicalExtend(const string& queryRel);

    unique_ptr<Schema> copy();

public:
    vector<unique_ptr<FactorizationGroup>> groups;
    // Maps a queryRel to the LogicalExtend that matches it. This is needed because ScanRelProperty
    // requires direction information which only available in the LogicalExtend.
    unordered_map<string, LogicalExtend*> queryRelLogicalExtendMap;
    // All flat variables are considered as in the same factorization group
    unordered_map<string, uint32_t> variableToGroupPos;
};

} // namespace planner
} // namespace graphflow
