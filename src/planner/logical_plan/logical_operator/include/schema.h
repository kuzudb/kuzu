#pragma once

#include <unordered_map>

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;
using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalExtend;

class FactorizationGroup {
    friend class Schema;

public:
    FactorizationGroup() : isFlat{false}, estimatedCardinality{1} {}

    FactorizationGroup(const FactorizationGroup& other)
        : isFlat{other.isFlat}, estimatedCardinality{other.estimatedCardinality},
          expressions{other.expressions} {}

    inline void setIsFlat(bool flag) { isFlat = flag; }

    inline bool getIsFlat() const { return isFlat; }

    inline void setEstimatedCardinality(uint64_t cardinality) {
        estimatedCardinality = cardinality;
    }

    inline uint64_t getEstimatedCardinality() const { return estimatedCardinality; }

    inline void insertExpression(const shared_ptr<Expression>& expression) {
        expressions.push_back(expression);
    }

    inline shared_ptr<Expression> getAnyExpression() const {
        assert(!expressions.empty());
        return expressions[0];
    }

    inline expression_vector getExpressions() const { return expressions; }

    inline uint32_t getNumExpressions() const { return expressions.size(); }

private:
    bool isFlat;
    uint64_t estimatedCardinality;
    expression_vector expressions;
};

class Schema {

public:
    inline uint32_t getNumGroups() const { return groups.size(); }

    inline FactorizationGroup* getGroup(const string& expressionName) const {
        return getGroup(getGroupPos(expressionName));
    }

    inline FactorizationGroup* getGroup(uint32_t pos) const { return groups[pos].get(); }

    uint32_t createGroup();

    void insertToGroupAndScope(const shared_ptr<Expression>& expression, uint32_t groupPos);

    void insertToGroupAndScope(const expression_vector& expressions, uint32_t groupPos);

    uint32_t getGroupPos(const string& expressionName) const;

    inline void flattenGroup(uint32_t pos) { groups[pos]->isFlat = true; }

    bool isExpressionInScope(const Expression& expression) const;

    inline expression_vector getExpressionsInScope() const { return expressionsInScope; }

    expression_vector getExpressionsInScope(uint32_t pos) const;

    inline void clearExpressionsInScope() { expressionsInScope.clear(); }

    // Get the group positions containing at least one expression in scope.
    unordered_set<uint32_t> getGroupsPosInScope() const;

    void addLogicalExtend(const string& queryRel, LogicalExtend* extend);

    LogicalExtend* getExistingLogicalExtend(const string& queryRel);

    unique_ptr<Schema> copy() const;

    void clear();

private:
    vector<unique_ptr<FactorizationGroup>> groups;
    unordered_map<string, uint32_t> expressionNameToGroupPos;
    // Maps a queryRel to the LogicalExtend that matches it. This is needed because ScanRelProperty
    // requires direction information which only available in the LogicalExtend.
    unordered_map<string, LogicalExtend*> queryRelLogicalExtendMap;
    // Our projection doesn't explicitly remove expressions. Instead, we keep track of what
    // expressions are in scope (i.e. being projected).
    expression_vector expressionsInScope;
};

} // namespace planner
} // namespace graphflow
