#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/binder/expression/include/expression.h"

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
        assert(!expressionNames.empty());
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

    inline FactorizationGroup* getGroup(const string& expressionName) const {
        return getGroup(getGroupPos(expressionName));
    }

    inline FactorizationGroup* getGroup(uint32_t pos) const { return groups[pos].get(); }

    uint32_t createGroup();

    void insertToGroup(const string& expressionName, uint32_t groupPos);

    void insertToGroupAndScope(const string& expressionName, uint32_t groupPos);

    void insertToGroupAndScope(const unordered_set<string>& expressionNames, uint32_t groupPos);

    uint32_t getGroupPos(const string& expressionName) const;

    inline void flattenGroup(uint32_t pos) { groups[pos]->isFlat = true; }

    inline bool expressionInScope(const string& expressionName) const {
        return expressionNamesInScope.contains(expressionName);
    }

    inline unordered_set<string> getExpressionNamesInScope() const {
        return expressionNamesInScope;
    }

    unordered_set<string> getExpressionNamesInScope(uint32_t pos) const;

    void removeExpression(const string& expressionName);

    inline void clearExpressionsInScope() { expressionNamesInScope.clear(); }

    // Get the group positions containing at least one expression in scope.
    unordered_set<uint32_t> getGroupsPosInScope() const;

    void addLogicalExtend(const string& queryRel, LogicalExtend* extend);

    inline bool containLogicalExtend(const string& queryRel) {
        return queryRelLogicalExtendMap.contains(queryRel);
    }
    LogicalExtend* getExistingLogicalExtend(const string& queryRel);

    unique_ptr<Schema> copy() const;

    void clear();

public:
    vector<unique_ptr<FactorizationGroup>> groups;
    // Maps a queryRel to the LogicalExtend that matches it. This is needed because ScanRelProperty
    // requires direction information which only available in the LogicalExtend.
    unordered_map<string, LogicalExtend*> queryRelLogicalExtendMap;

private:
    unordered_map<string, uint32_t> expressionNameToGroupPos;
    // Our projection doesn't explicitly remove expressions. Instead, we keep track of what
    // expressions are in scope (i.e. being projected).
    unordered_set<string> expressionNamesInScope;
};

} // namespace planner
} // namespace graphflow
