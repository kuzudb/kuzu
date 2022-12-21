#pragma once

#include <unordered_map>

#include "binder/expression/expression.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

class FactorizationGroup {
    friend class Schema;

public:
    FactorizationGroup() : flat{false}, singleState{false}, cardinalityMultiplier{1} {}
    FactorizationGroup(const FactorizationGroup& other)
        : flat{other.flat}, singleState{other.singleState},
          cardinalityMultiplier{other.cardinalityMultiplier}, expressions{other.expressions},
          expressionNameToPos{other.expressionNameToPos} {}

    inline void setFlat() {
        assert(!flat);
        flat = true;
    }
    inline bool isFlat() const { return flat; }
    inline void setSingleState() {
        assert(!singleState);
        singleState = true;
        setFlat();
    }
    inline bool isSingleState() const { return singleState; }

    inline void setMultiplier(uint64_t multiplier) { cardinalityMultiplier = multiplier; }
    inline uint64_t getMultiplier() const { return cardinalityMultiplier; }

    inline void insertExpression(const shared_ptr<Expression>& expression) {
        assert(!expressionNameToPos.contains(expression->getUniqueName()));
        expressionNameToPos.insert({expression->getUniqueName(), expressions.size()});
        expressions.push_back(expression);
    }
    inline expression_vector getExpressions() const { return expressions; }
    inline uint32_t getExpressionPos(const Expression& expression) {
        assert(expressionNameToPos.contains(expression.getUniqueName()));
        return expressionNameToPos.at(expression.getUniqueName());
    }

private:
    bool flat;
    bool singleState;
    uint64_t cardinalityMultiplier;
    expression_vector expressions;
    unordered_map<string, uint32_t> expressionNameToPos;
};

class Schema {
public:
    inline uint32_t getNumGroups() const { return groups.size(); }

    inline FactorizationGroup* getGroup(shared_ptr<Expression> expression) const {
        return getGroup(getGroupPos(expression->getUniqueName()));
    }

    inline FactorizationGroup* getGroup(const string& expressionName) const {
        return getGroup(getGroupPos(expressionName));
    }

    inline FactorizationGroup* getGroup(uint32_t pos) const { return groups[pos].get(); }

    uint32_t createGroup();

    void insertToScope(const shared_ptr<Expression>& expression, uint32_t groupPos);

    void insertToGroupAndScope(const shared_ptr<Expression>& expression, uint32_t groupPos);

    void insertToGroupAndScope(const expression_vector& expressions, uint32_t groupPos);

    inline uint32_t getGroupPos(const Expression& expression) const {
        return getGroupPos(expression.getUniqueName());
    }

    inline uint32_t getGroupPos(const string& expressionName) const {
        assert(expressionNameToGroupPos.contains(expressionName));
        return expressionNameToGroupPos.at(expressionName);
    }

    inline pair<uint32_t, uint32_t> getExpressionPos(const Expression& expression) const {
        auto groupPos = getGroupPos(expression);
        return make_pair(groupPos, groups[groupPos]->getExpressionPos(expression));
    }

    inline void flattenGroup(uint32_t pos) { groups[pos]->setFlat(); }
    inline void setGroupAsSingleState(uint32_t pos) { groups[pos]->setSingleState(); }

    bool isExpressionInScope(const Expression& expression) const;

    inline expression_vector getExpressionsInScope() const { return expressionsInScope; }

    expression_vector getExpressionsInScope(uint32_t pos) const;

    expression_vector getSubExpressionsInScope(const shared_ptr<Expression>& expression);

    unordered_set<uint32_t> getDependentGroupsPos(const shared_ptr<Expression>& expression);

    inline void clearExpressionsInScope() {
        expressionNameToGroupPos.clear();
        expressionsInScope.clear();
    }

    // Get the group positions containing at least one expression in scope.
    unordered_set<uint32_t> getGroupsPosInScope() const;

    unique_ptr<Schema> copy() const;

    void clear();

private:
    vector<unique_ptr<FactorizationGroup>> groups;
    unordered_map<string, uint32_t> expressionNameToGroupPos;
    // Our projection doesn't explicitly remove expressions. Instead, we keep track of what
    // expressions are in scope (i.e. being projected).
    expression_vector expressionsInScope;
};

} // namespace planner
} // namespace kuzu
