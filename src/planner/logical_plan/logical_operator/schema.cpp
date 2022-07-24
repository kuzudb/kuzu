#include "src/planner/logical_plan/logical_operator/include/schema.h"

namespace graphflow {
namespace planner {

uint32_t Schema::createGroup() {
    auto pos = groups.size();
    groups.push_back(make_unique<FactorizationGroup>());
    return pos;
}

void Schema::insertToGroupAndScope(const shared_ptr<Expression>& expression, uint32_t groupPos) {
    expressionNameToGroupPos.insert({expression->getUniqueName(), groupPos});
    groups[groupPos]->insertExpression(expression);
    expressionsInScope.push_back(expression);
}

void Schema::insertToGroupAndScope(const expression_vector& expressions, uint32_t groupPos) {
    for (auto& expression : expressions) {
        insertToGroupAndScope(expression, groupPos);
    }
}

uint32_t Schema::getGroupPos(const string& expressionName) const {
    assert(expressionNameToGroupPos.contains(expressionName));
    return expressionNameToGroupPos.at(expressionName);
}

bool Schema::isExpressionInScope(const Expression& expression) const {
    for (auto& expressionInScope : expressionsInScope) {
        if (expressionInScope->getUniqueName() == expression.getUniqueName()) {
            return true;
        }
    }
    return false;
}

expression_vector Schema::getExpressionsInScope(uint32_t pos) const {
    expression_vector result;
    for (auto& expression : expressionsInScope) {
        if (getGroupPos(expression->getUniqueName()) == pos) {
            result.push_back(expression);
        }
    }
    return result;
}

unordered_set<uint32_t> Schema::getGroupsPosInScope() const {
    unordered_set<uint32_t> result;
    for (auto& expressionInScope : expressionsInScope) {
        result.insert(getGroupPos(expressionInScope->getUniqueName()));
    }
    return result;
}

unique_ptr<Schema> Schema::copy() const {
    auto newSchema = make_unique<Schema>();
    newSchema->expressionNameToGroupPos = expressionNameToGroupPos;
    for (auto& group : groups) {
        newSchema->groups.push_back(make_unique<FactorizationGroup>(*group));
    }
    newSchema->expressionsInScope = expressionsInScope;
    return newSchema;
}

void Schema::clear() {
    groups.clear();
    expressionNameToGroupPos.clear();
    clearExpressionsInScope();
}

} // namespace planner
} // namespace graphflow
