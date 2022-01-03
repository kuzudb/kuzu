#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/binder/include/expression/expression.h"
#include "src/common/include/assert.h"

using namespace graphflow::binder;
using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalExtend;

class FactorizationGroup {

public:
    FactorizationGroup() : isFlat{false}, estimatedCardinality{1} {}

    FactorizationGroup(const FactorizationGroup& other)
        : isFlat{other.isFlat}, estimatedCardinality{other.estimatedCardinality},
          expressionNames{other.expressionNames} {}

    inline void insertExpression(const string& expressionName) {
        expressionNames.insert(expressionName);
    }

    inline void removeExpression(const string& expressionName) {
        expressionNames.erase(expressionName);
    }

    inline string getAnyExpressionName() {
        GF_ASSERT(!expressionNames.empty());
        return *expressionNames.begin();
    }

    inline uint32_t getNumExpressions() const { return expressionNames.size(); }

public:
    bool isFlat;
    uint64_t estimatedCardinality;
    unordered_set<string> expressionNames;
};

class Schema {

public:
    inline uint32_t getNumGroups() const { return groups.size(); }
    inline FactorizationGroup* getGroup(uint32_t pos) const { return groups[pos].get(); }

    uint32_t createGroup();

    void insertToGroup(const string& expressionName, uint32_t groupPos);

    void insertToGroup(const FactorizationGroup& otherGroup, uint32_t groupPos);

    uint32_t getGroupPos(const string& expressionName) const;

    unordered_set<uint32_t> getGroupsPos() const;

    void flattenGroup(uint32_t pos);

    bool containExpression(const string& expressionName) const {
        return expressionNameToGroupPos.contains(expressionName);
    }

    void removeExpression(const string& expressionName) {
        auto groupPos = getGroupPos(expressionName);
        groups[groupPos]->removeExpression(expressionName);
        expressionNameToGroupPos.erase(expressionName);
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
    unordered_map<string, uint32_t> expressionNameToGroupPos;
    vector<shared_ptr<Expression>> expressionsToCollect;
};

} // namespace planner
} // namespace graphflow
