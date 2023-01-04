#include "planner/logical_plan/logical_operator/schema.h"

namespace kuzu {
namespace planner {

f_group_pos Schema::createGroup() {
    auto pos = groups.size();
    groups.push_back(make_unique<FactorizationGroup>());
    return pos;
}

void Schema::insertToScope(const shared_ptr<Expression>& expression, f_group_pos groupPos) {
    assert(!expressionNameToGroupPos.contains(expression->getUniqueName()));
    expressionNameToGroupPos.insert({expression->getUniqueName(), groupPos});
    expressionsInScope.push_back(expression);
}

void Schema::insertToGroupAndScope(const shared_ptr<Expression>& expression, f_group_pos groupPos) {
    assert(!expressionNameToGroupPos.contains(expression->getUniqueName()));
    expressionNameToGroupPos.insert({expression->getUniqueName(), groupPos});
    groups[groupPos]->insertExpression(expression);
    expressionsInScope.push_back(expression);
}

void Schema::insertToGroupAndScope(const expression_vector& expressions, f_group_pos groupPos) {
    for (auto& expression : expressions) {
        insertToGroupAndScope(expression, groupPos);
    }
}

bool Schema::isExpressionInScope(const Expression& expression) const {
    for (auto& expressionInScope : expressionsInScope) {
        if (expressionInScope->getUniqueName() == expression.getUniqueName()) {
            return true;
        }
    }
    return false;
}

expression_vector Schema::getExpressionsInScope(f_group_pos pos) const {
    expression_vector result;
    for (auto& expression : expressionsInScope) {
        if (getGroupPos(expression->getUniqueName()) == pos) {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector Schema::getSubExpressionsInScope(const shared_ptr<Expression>& expression) {
    expression_vector results;
    if (isExpressionInScope(*expression)) {
        results.push_back(expression);
        return results;
    }
    for (auto& child : expression->getChildren()) {
        for (auto& subExpression : getSubExpressionsInScope(child)) {
            results.push_back(subExpression);
        }
    }
    return results;
}

unordered_set<f_group_pos> Schema::getDependentGroupsPos(const shared_ptr<Expression>& expression) {
    unordered_set<f_group_pos> result;
    for (auto& subExpression : getSubExpressionsInScope(expression)) {
        result.insert(getGroupPos(subExpression->getUniqueName()));
    }
    return result;
}

unordered_set<f_group_pos> Schema::getGroupsPosInScope() const {
    unordered_set<f_group_pos> result;
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
    clearExpressionsInScope();
}

vector<expression_vector> SchemaUtils::getExpressionsPerGroup(
    const binder::expression_vector& expressions, const Schema& schema) {
    vector<expression_vector> result;
    result.resize(schema.getNumGroups());
    for (auto& expression : expressions) {
        auto groupPos = schema.getGroupPos(*expression);
        result[groupPos].push_back(expression);
    }
    return result;
}

} // namespace planner
} // namespace kuzu
