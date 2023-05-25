#include "binder/visitor/property_collector.h"

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
        collectPropertyExpressions(*matchClause.getWhereExpression());
    }
}

void PropertyCollector::visitUnwind(const BoundReadingClause& readingClause) {
    auto& unwindClause = (BoundUnwindClause&)readingClause;
    collectPropertyExpressions(*unwindClause.getExpression());
}

void PropertyCollector::visitSet(const BoundUpdatingClause& updatingClause) {
    auto& boundSetClause = (BoundSetClause&)updatingClause;
    for (auto& setNodeProperty : boundSetClause.getSetNodeProperties()) {
        collectPropertyExpressions(*setNodeProperty->getSetItem().second);
    }
    for (auto& setRelProperty : boundSetClause.getSetRelProperties()) {
        collectPropertyExpressions(*setRelProperty->getSetItem().second);
    }
}

void PropertyCollector::visitDelete(const BoundUpdatingClause& updatingClause) {
    auto& boundDeleteClause = (BoundDeleteClause&)updatingClause;
    for (auto& deleteNode : boundDeleteClause.getDeleteNodes()) {
        properties.insert(deleteNode->getPrimaryKeyExpression());
    }
    for (auto& deleteRel : boundDeleteClause.getDeleteRels()) {
        properties.insert(deleteRel->getInternalIDProperty());
    }
}

void PropertyCollector::visitCreate(const BoundUpdatingClause& updatingClause) {
    auto& boundCreateClause = (BoundCreateClause&)updatingClause;
    for (auto& createNode : boundCreateClause.getCreateNodes()) {
        for (auto& setItem : createNode->getSetItems()) {
            collectPropertyExpressions(*setItem.second);
        }
    }
    for (auto& createRel : boundCreateClause.getCreateRels()) {
        for (auto& setItem : createRel->getSetItems()) {
            collectPropertyExpressions(*setItem.second);
        }
    }
}

void PropertyCollector::visitProjectionBody(const BoundProjectionBody& projectionBody) {
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        collectPropertyExpressions(*expression);
    }
    for (auto& expression : projectionBody.getOrderByExpressions()) {
        collectPropertyExpressions(*expression);
    }
}

void PropertyCollector::visitProjectionBodyPredicate(Expression& predicate) {
    collectPropertyExpressions(predicate);
}

void PropertyCollector::collectPropertyExpressions(Expression& expression) {
    for (auto& property : expression.getSubPropertyExpressions()) {
        properties.insert(property);
    }
}

} // namespace binder
} // namespace kuzu
