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
        if (rel->isEmpty()) {
            // If a query rel is empty then it does not have an internal id property.
            continue;
        }
        if (rel->getRelType() == QueryRelType::NON_RECURSIVE) {
            properties.insert(rel->getInternalIDProperty());
        }
    }
    if (matchClause.hasPredicate()) {
        collectPropertyExpressions(matchClause.getPredicate());
    }
}

void PropertyCollector::visitUnwind(const BoundReadingClause& readingClause) {
    auto& unwindClause = (BoundUnwindClause&)readingClause;
    collectPropertyExpressions(unwindClause.getInExpr());
}

void PropertyCollector::visitLoadFrom(const BoundReadingClause& readingClause) {
    auto& loadFromClause =
        ku_dynamic_cast<const BoundReadingClause&, const BoundLoadFrom&>(readingClause);
    if (loadFromClause.hasPredicate()) {
        collectPropertyExpressions(loadFromClause.getPredicate());
    }
}

void PropertyCollector::visitInQueryCall(const BoundReadingClause& readingClause) {
    auto& inQueryCallClause =
        ku_dynamic_cast<const BoundReadingClause&, const BoundInQueryCall&>(readingClause);
    if (inQueryCallClause.hasPredicate()) {
        collectPropertyExpressions(inQueryCallClause.getPredicate());
    }
}

void PropertyCollector::visitSet(const BoundUpdatingClause& updatingClause) {
    auto& boundSetClause = (BoundSetClause&)updatingClause;
    for (auto& info : boundSetClause.getInfosRef()) {
        collectPropertyExpressions(info.setItem.second);
    }
}

void PropertyCollector::visitDelete(const BoundUpdatingClause& updatingClause) {
    auto& boundDeleteClause = updatingClause.constCast<BoundDeleteClause>();
    // Read primary key if we are deleting nodes;
    for (auto& info : boundDeleteClause.getNodeInfos()) {
        auto& node = info.pattern->constCast<NodeExpression>();
        for (auto id : node.getTableIDs()) {
            properties.insert(node.getPrimaryKey(id));
        }
    }
    // Read rel internal id if we are deleting relationships.
    for (auto& info : boundDeleteClause.getRelInfos()) {
        auto& rel = info.pattern->constCast<RelExpression>();
        properties.insert(rel.getInternalIDProperty());
    }
}

void PropertyCollector::visitInsert(const BoundUpdatingClause& updatingClause) {
    auto& insertClause = (BoundInsertClause&)updatingClause;
    for (auto& info : insertClause.getInfos()) {
        for (auto& expr : info.columnDataExprs) {
            collectPropertyExpressions(expr);
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
        for (auto& expr : info.columnDataExprs) {
            collectPropertyExpressions(expr);
        }
    }
    for (auto& info : boundMergeClause.getOnMatchSetInfosRef()) {
        collectPropertyExpressions(info.setItem.second);
    }
    for (auto& info : boundMergeClause.getOnCreateSetInfosRef()) {
        collectPropertyExpressions(info.setItem.second);
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
