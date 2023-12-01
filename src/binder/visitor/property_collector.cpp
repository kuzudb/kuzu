#include "binder/visitor/property_collector.h"

#include "binder/expression_visitor.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_insert_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"

using namespace kuzu::common;

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
        if (rel->getRelType() == QueryRelType::NON_RECURSIVE) {
            properties.insert(rel->getInternalIDProperty());
        }
    }
    if (matchClause.hasWherePredicate()) {
        collectPropertyExpressions(matchClause.getWherePredicate());
    }
}

void PropertyCollector::visitUnwind(const BoundReadingClause& readingClause) {
    auto& unwindClause = (BoundUnwindClause&)readingClause;
    collectPropertyExpressions(unwindClause.getExpression());
}

void PropertyCollector::visitLoadFrom(const BoundReadingClause& readingClause) {
    auto& loadFromClause = reinterpret_cast<const BoundLoadFrom&>(readingClause);
    if (loadFromClause.hasWherePredicate()) {
        collectPropertyExpressions(loadFromClause.getWherePredicate());
    }
}

void PropertyCollector::visitInQueryCall(const BoundReadingClause& readingClause) {
    auto& inQueryCallClause = reinterpret_cast<const BoundInQueryCall&>(readingClause);
    if (inQueryCallClause.hasWherePredicate()) {
        collectPropertyExpressions(inQueryCallClause.getWherePredicate());
    }
}

void PropertyCollector::visitSet(const BoundUpdatingClause& updatingClause) {
    auto& boundSetClause = (BoundSetClause&)updatingClause;
    for (auto& info : boundSetClause.getInfosRef()) {
        collectPropertyExpressions(info->setItem.second);
    }
}

void PropertyCollector::visitDelete(const BoundUpdatingClause& updatingClause) {
    auto& boundDeleteClause = (BoundDeleteClause&)updatingClause;
    for (auto& info : boundDeleteClause.getRelInfos()) {
        auto rel = (RelExpression*)info->nodeOrRel.get();
        properties.insert(rel->getInternalIDProperty());
    }
}

void PropertyCollector::visitInsert(const BoundUpdatingClause& updatingClause) {
    auto& insertClause = (BoundInsertClause&)updatingClause;
    for (auto& info : insertClause.getInfosRef()) {
        for (auto& setItem : info->setItems) {
            collectPropertyExpressions(setItem.second);
        }
    }
}

void PropertyCollector::visitMerge(const BoundUpdatingClause& updatingClause) {
    auto& boundMergeClause = (BoundMergeClause&)updatingClause;
    for (auto& rel : boundMergeClause.getQueryGraphCollection()->getQueryRels()) {
        if (rel->getRelType() == QueryRelType::NON_RECURSIVE) {
            properties.insert(rel->getInternalIDProperty());
        }
    }
    if (boundMergeClause.hasPredicate()) {
        collectPropertyExpressions(boundMergeClause.getPredicate());
    }
    for (auto& info : boundMergeClause.getInsertInfosRef()) {
        for (auto& setItem : info->setItems) {
            collectPropertyExpressions(setItem.second);
        }
    }
    for (auto& info : boundMergeClause.getOnMatchSetInfosRef()) {
        collectPropertyExpressions(info->setItem.second);
    }
    for (auto& info : boundMergeClause.getOnCreateSetInfosRef()) {
        collectPropertyExpressions(info->setItem.second);
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
