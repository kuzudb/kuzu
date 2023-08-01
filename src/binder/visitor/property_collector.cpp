#include "binder/visitor/property_collector.h"

#include "binder/expression/expression_visitor.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"

namespace kuzu {
namespace binder {

expression_vector PropertyCollector::getProperties() {
    expression_vector result;
    for (auto& property : properties) {
        result.push_back(property);
    }
    return result;
}

void PropertyCollector::visitMatch(const BoundReadingClause& readingClause) {
    auto& matchClause = (BoundMatchClause&)readingClause;
    for (auto& rel : matchClause.getQueryGraphCollection()->getQueryRels()) {
        if (rel->getRelType() == common::QueryRelType::NON_RECURSIVE) {
            properties.insert(rel->getInternalIDProperty());
        }
    }
    if (matchClause.hasWhereExpression()) {
        collectPropertyExpressions(matchClause.getWhereExpression());
    }
}

void PropertyCollector::visitUnwind(const BoundReadingClause& readingClause) {
    auto& unwindClause = (BoundUnwindClause&)readingClause;
    collectPropertyExpressions(unwindClause.getExpression());
}

void PropertyCollector::visitSet(const BoundUpdatingClause& updatingClause) {
    auto& boundSetClause = (BoundSetClause&)updatingClause;
    for (auto& info : boundSetClause.getInfosRef()) {
        collectPropertyExpressions(info->setItem.second);
    }
}

void PropertyCollector::visitDelete(const BoundUpdatingClause& updatingClause) {
    auto& boundDeleteClause = (BoundDeleteClause&)updatingClause;
    for (auto& info : boundDeleteClause.getNodeInfos()) {
        auto extraInfo = (ExtraDeleteNodeInfo*)info->extraInfo.get();
        properties.insert(extraInfo->primaryKey);
    }
    for (auto& info : boundDeleteClause.getRelInfos()) {
        auto rel = (RelExpression*)info->nodeOrRel.get();
        properties.insert(rel->getInternalIDProperty());
    }
}

void PropertyCollector::visitCreate(const BoundUpdatingClause& updatingClause) {
    auto& boundCreateClause = (BoundCreateClause&)updatingClause;
    for (auto& info : boundCreateClause.getInfosRef()) {
        for (auto& setItem : info->setItems) {
            collectPropertyExpressions(setItem.second);
        }
    }
}

void PropertyCollector::visitProjectionBody(const BoundProjectionBody& projectionBody) {
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        collectPropertyExpressions(expression);
    }
    for (auto& expression : projectionBody.getOrderByExpressions()) {
        collectPropertyExpressions(expression);
    }
}

void PropertyCollector::visitProjectionBodyPredicate(const std::shared_ptr<Expression>& predicate) {
    collectPropertyExpressions(predicate);
}

void PropertyCollector::collectPropertyExpressions(const std::shared_ptr<Expression>& expression) {
    auto expressionCollector = std::make_unique<binder::ExpressionCollector>();
    for (auto& property : expressionCollector->collectPropertyExpressions(expression)) {
        properties.insert(property);
    }
}

} // namespace binder
} // namespace kuzu
