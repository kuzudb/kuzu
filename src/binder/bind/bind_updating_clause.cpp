#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_insert_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "catalog/node_table_schema.h"
#include "common/assert.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "parser/query/updating_clause/delete_clause.h"
#include "parser/query/updating_clause/insert_clause.h"
#include "parser/query/updating_clause/merge_clause.h"
#include "parser/query/updating_clause/set_clause.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundUpdatingClause> Binder::bindUpdatingClause(
    const UpdatingClause& updatingClause) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::INSERT: {
        return bindInsertClause(updatingClause);
    }
    case ClauseType::MERGE: {
        return bindMergeClause(updatingClause);
    }
    case ClauseType::SET: {
        return bindSetClause(updatingClause);
    }
    case ClauseType::DELETE_: {
        return bindDeleteClause(updatingClause);
    }
    default:
        KU_UNREACHABLE;
    }
}

static expression_set populateNodeRelScope(const BinderScope& scope) {
    expression_set result;
    for (auto& expression : scope.getExpressions()) {
        if (ExpressionUtil::isNodePattern(*expression) ||
            ExpressionUtil::isRelPattern(*expression)) {
            result.insert(expression);
        }
    }
    return result;
}

std::unique_ptr<BoundUpdatingClause> Binder::bindInsertClause(
    const UpdatingClause& updatingClause) {
    auto& insertClause = (InsertClause&)updatingClause;
    auto nodeRelScope = populateNodeRelScope(*scope);
    // bindGraphPattern will update scope.
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(insertClause.getPatternElementsRef());
    auto createInfos = bindCreateInfos(*queryGraphCollection, *propertyCollection, nodeRelScope);
    return std::make_unique<BoundInsertClause>(std::move(createInfos));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindMergeClause(
    const parser::UpdatingClause& updatingClause) {
    auto& mergeClause = (MergeClause&)updatingClause;
    auto nodeRelScope = populateNodeRelScope(*scope);
    // bindGraphPattern will update scope.
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(mergeClause.getPatternElementsRef());
    std::shared_ptr<Expression> predicate = nullptr;
    for (auto& [key, val] : propertyCollection->getKeyVals()) {
        predicate = expressionBinder.combineBooleanExpressions(ExpressionType::AND,
            expressionBinder.createEqualityComparisonExpression(key, val), predicate);
    }
    auto createInfos = bindCreateInfos(*queryGraphCollection, *propertyCollection, nodeRelScope);
    auto boundMergeClause = std::make_unique<BoundMergeClause>(
        std::move(queryGraphCollection), std::move(predicate), std::move(createInfos));
    if (mergeClause.hasOnMatchSetItems()) {
        for (auto& [lhs, rhs] : mergeClause.getOnMatchSetItemsRef()) {
            auto setPropertyInfo = bindSetPropertyInfo(lhs.get(), rhs.get());
            boundMergeClause->addOnMatchSetPropertyInfo(std::move(setPropertyInfo));
        }
    }
    if (mergeClause.hasOnCreateSetItems()) {
        for (auto& [lhs, rhs] : mergeClause.getOnCreateSetItemsRef()) {
            auto setPropertyInfo = bindSetPropertyInfo(lhs.get(), rhs.get());
            boundMergeClause->addOnCreateSetPropertyInfo(std::move(setPropertyInfo));
        }
    }
    return boundMergeClause;
}

std::vector<std::unique_ptr<BoundInsertInfo>> Binder::bindCreateInfos(
    const QueryGraphCollection& queryGraphCollection,
    const PropertyKeyValCollection& keyValCollection, const expression_set& nodeRelScope_) {
    auto nodeRelScope = nodeRelScope_;
    std::vector<std::unique_ptr<BoundInsertInfo>> result;
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection.getQueryGraph(i);
        for (auto j = 0u; j < queryGraph->getNumQueryNodes(); ++j) {
            auto node = queryGraph->getQueryNode(j);
            if (nodeRelScope.contains(node)) {
                continue;
            }
            nodeRelScope.insert(node);
            result.push_back(bindInsertNodeInfo(node, keyValCollection));
        }
        for (auto j = 0u; j < queryGraph->getNumQueryRels(); ++j) {
            auto rel = queryGraph->getQueryRel(j);
            if (nodeRelScope.contains(rel)) {
                continue;
            }
            nodeRelScope.insert(rel);
            result.push_back(bindInsertRelInfo(rel, keyValCollection));
        }
    }
    if (result.empty()) {
        throw BinderException("Cannot resolve any node or relationship to create.");
    }
    return result;
}

static void validatePrimaryKeyExistence(const PropertyKeyValCollection& collection,
    TableSchema* tableSchema, const std::shared_ptr<Expression>& node) {
    auto tableID = tableSchema->getTableID();
    auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(tableSchema);
    auto primaryKey = nodeTableSchema->getPrimaryKey();
    std::shared_ptr<Expression> primaryKeyExpression;
    for (auto& [key, val] : collection.getKeyVals(node)) {
        auto propertyExpression = static_pointer_cast<PropertyExpression>(key);
        if (propertyExpression->getPropertyID(tableID) == primaryKey->getPropertyID()) {
            primaryKeyExpression = val;
        }
    }
    if (primaryKey->getDataType()->getLogicalTypeID() != LogicalTypeID::SERIAL &&
        primaryKeyExpression == nullptr) {
        throw BinderException("Create node " + node->toString() + " expects primary key " +
                              primaryKey->getName() + " as input.");
    }
}

std::unique_ptr<BoundInsertInfo> Binder::bindInsertNodeInfo(
    std::shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Create node " + node->toString() + " with multiple node labels is not supported.");
    }
    auto tableID = node->getSingleTableID();
    auto tableSchema = catalog.getReadOnlyVersion()->getTableSchema(tableID);
    validatePrimaryKeyExistence(collection, tableSchema, node);
    auto setItems = bindSetItems(collection, tableSchema, node);
    return std::make_unique<BoundInsertInfo>(
        UpdateTableType::NODE, std::move(node), std::move(setItems));
}

std::unique_ptr<BoundInsertInfo> Binder::bindInsertRelInfo(
    std::shared_ptr<RelExpression> rel, const PropertyKeyValCollection& collection) {
    if (rel->isMultiLabeled() || rel->isBoundByMultiLabeledNode()) {
        throw BinderException(
            "Create rel " + rel->toString() +
            " with multiple rel labels or bound by multiple node labels is not supported.");
    }
    if (ExpressionUtil::isRecursiveRelPattern(*rel)) {
        throw BinderException(
            common::stringFormat("Cannot create recursive rel {}.", rel->toString()));
    }
    auto relTableID = rel->getSingleTableID();
    auto tableSchema = catalog.getReadOnlyVersion()->getTableSchema(relTableID);
    auto setItems = bindSetItems(collection, tableSchema, rel);
    return std::make_unique<BoundInsertInfo>(
        UpdateTableType::REL, std::move(rel), std::move(setItems));
}

std::vector<expression_pair> Binder::bindSetItems(const PropertyKeyValCollection& collection,
    TableSchema* tableSchema, const std::shared_ptr<Expression>& nodeOrRel) {
    std::vector<expression_pair> setItems;
    for (auto& property : tableSchema->getProperties()) {
        if (collection.hasKeyVal(nodeOrRel, property->getName())) { // input specifies rhs.
            setItems.emplace_back(collection.getKeyVal(nodeOrRel, property->getName()));
            continue;
        }
        auto propertyExpression =
            expressionBinder.bindNodeOrRelPropertyExpression(*nodeOrRel, property->getName());
        auto nullExpression = expressionBinder.createNullLiteralExpression();
        nullExpression =
            ExpressionBinder::implicitCastIfNecessary(nullExpression, propertyExpression->dataType);
        setItems.emplace_back(std::move(propertyExpression), std::move(nullExpression));
    }
    return setItems;
}

std::unique_ptr<BoundUpdatingClause> Binder::bindSetClause(const UpdatingClause& updatingClause) {
    auto& setClause = (SetClause&)updatingClause;
    auto boundSetClause = std::make_unique<BoundSetClause>();
    for (auto& setItem : setClause.getSetItemsRef()) {
        boundSetClause->addInfo(bindSetPropertyInfo(setItem.first.get(), setItem.second.get()));
    }
    return boundSetClause;
}

std::unique_ptr<BoundSetPropertyInfo> Binder::bindSetPropertyInfo(
    parser::ParsedExpression* lhs, parser::ParsedExpression* rhs) {
    auto boundLhs = expressionBinder.bindExpression(*lhs->getChild(0));
    if (ExpressionUtil::isNodePattern(*boundLhs)) {
        return std::make_unique<BoundSetPropertyInfo>(
            UpdateTableType::NODE, boundLhs, bindSetItem(lhs, rhs));
    } else if (ExpressionUtil::isRelPattern(*boundLhs)) {
        return std::make_unique<BoundSetPropertyInfo>(
            UpdateTableType::REL, boundLhs, bindSetItem(lhs, rhs));
    } else {
        throw BinderException(
            stringFormat("Cannot set expression {} with type {}. Expect node or rel pattern.",
                boundLhs->toString(), expressionTypeToString(boundLhs->expressionType)));
    }
}

expression_pair Binder::bindSetItem(parser::ParsedExpression* lhs, parser::ParsedExpression* rhs) {
    auto boundLhs = expressionBinder.bindExpression(*lhs);
    auto boundRhs = expressionBinder.bindExpression(*rhs);
    boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
    return make_pair(std::move(boundLhs), std::move(boundRhs));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindDeleteClause(
    const UpdatingClause& updatingClause) {
    auto& deleteClause = (DeleteClause&)updatingClause;
    auto boundDeleteClause = std::make_unique<BoundDeleteClause>();
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto nodeOrRel = expressionBinder.bindExpression(*deleteClause.getExpression(i));
        if (ExpressionUtil::isNodePattern(*nodeOrRel)) {
            auto deleteNodeInfo = std::make_unique<BoundDeleteInfo>(
                UpdateTableType::NODE, nodeOrRel, deleteClause.getDeleteClauseType());
            boundDeleteClause->addInfo(std::move(deleteNodeInfo));
        } else if (ExpressionUtil::isRelPattern(*nodeOrRel)) {
            if (deleteClause.getDeleteClauseType() == DeleteClauseType::DETACH_DELETE) {
                // TODO(Xiyang): Dummy check here. Make sure this is the correct semantic.
                throw BinderException("Detach delete on rel tables is not supported.");
            }
            auto rel = (RelExpression*)nodeOrRel.get();
            if (rel->getDirectionType() == RelDirectionType::BOTH) {
                throw BinderException("Delete undirected rel is not supported.");
            }
            auto deleteRel = std::make_unique<BoundDeleteInfo>(
                UpdateTableType::REL, nodeOrRel, DeleteClauseType::DELETE);
            boundDeleteClause->addInfo(std::move(deleteRel));
        } else {
            throw BinderException(stringFormat(
                "Cannot delete expression {} with type {}. Expect node or rel pattern.",
                nodeOrRel->toString(), expressionTypeToString(nodeOrRel->expressionType)));
        }
    }
    return boundDeleteClause;
}

} // namespace binder
} // namespace kuzu
