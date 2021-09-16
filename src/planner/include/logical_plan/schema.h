#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/common/include/assert.h"

using namespace graphflow::common;
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

    inline void insertVariable(const string& variable) { variables.insert(variable); }

    inline string getAnyVariable() {
        GF_ASSERT(!variables.empty());
        return *variables.begin();
    }

    inline uint32_t getNumVariables() const { return variables.size(); }

public:
    bool isFlat;
    uint64_t estimatedCardinality;
    unordered_set<string> variables;
};

class Schema {

public:
    inline uint32_t getNumGroups() const { return groups.size(); }
    inline FactorizationGroup* getGroup(uint32_t pos) const { return groups[pos].get(); }

    uint32_t createGroup();

    void insertToGroup(const string& variable, uint32_t groupPos);

    void insertToGroup(const FactorizationGroup& otherGroup, uint32_t groupPos);

    uint32_t getGroupPos(const string& variable) const;

    unordered_set<uint32_t> getUnFlatGroupsPos() const;

    uint32_t getAnyGroupPos() const;

    void flattenGroup(uint32_t pos);

    bool containVariable(const string& variable) const {
        return variableToGroupPos.contains(variable);
    }

    void addLogicalExtend(const string& queryRel, LogicalExtend* extend);

    inline bool containLogicalExtend(const string& queryRel) {
        return queryRelLogicalExtendMap.contains(queryRel);
    }
    LogicalExtend* getExistingLogicalExtend(const string& queryRel);

    unique_ptr<Schema> copy() const;

    void clearGroups();

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
