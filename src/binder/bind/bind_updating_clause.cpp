#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "catalog/node_table_schema.h"
#include "common/exception/binder.h"
#include "parser/query/updating_clause/create_clause.h"
#include "parser/query/updating_clause/delete_clause.h"
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
    case ClauseType::CREATE: {
        return bindCreateClause(updatingClause);
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
        throw NotImplementedException("Binder::bindUpdatingClause");
    }
}

static expression_set populateNodeRelScope(const BinderScope& scope) {
    expression_set result;
    for (auto& expression : scope.getExpressions()) {
        if (ExpressionUtil::isNodeVariable(*expression) ||
            ExpressionUtil::isRelVariable(*expression)) {
            result.insert(expression);
        }
    }
    return result;
}

std::unique_ptr<BoundUpdatingClause> Binder::bindCreateClause(
    const UpdatingClause& updatingClause) {
    auto& createClause = (CreateClause&)updatingClause;
    auto nodeRelScope = populateNodeRelScope(*scope);
    // bindGraphPattern will update scope.
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(createClause.getPatternElementsRef());
    auto createInfos = bindCreateInfos(*queryGraphCollection, *propertyCollection, nodeRelScope);
    return std::make_unique<BoundCreateClause>(std::move(createInfos));
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
        predicate = expressionBinder.combineConjunctiveExpressions(
            expressionBinder.createEqualityComparisonExpression(key, val), predicate);
    }
    auto createInfos = bindCreateInfos(*queryGraphCollection, *propertyCollection, nodeRelScope);
    auto boundMergeClause = std::make_unique<BoundMergeClause>(
        std::move(queryGraphCollection), std::move(predicate), std::move(createInfos));
    if (mergeClause.hasOnMatchSetItems()) {
        for (auto i = 0u; i < mergeClause.getNumOnMatchSetItems(); ++i) {
            auto setPropertyInfo = bindSetPropertyInfo(mergeClause.getOnMatchSetItem(i));
            boundMergeClause->addOnMatchSetPropertyInfo(std::move(setPropertyInfo));
        }
    }
    if (mergeClause.hasOnCreateSetItems()) {
        for (auto i = 0u; i < mergeClause.getNumOnCreateSetItems(); ++i) {
            auto setPropertyInfo = bindSetPropertyInfo(mergeClause.getOnCreateSetItem(i));
            boundMergeClause->addOnCreateSetPropertyInfo(std::move(setPropertyInfo));
        }
    }
    return boundMergeClause;
}

std::vector<std::unique_ptr<BoundCreateInfo>> Binder::bindCreateInfos(
    const QueryGraphCollection& queryGraphCollection,
    const PropertyKeyValCollection& keyValCollection, const expression_set& nodeRelScope_) {
    auto nodeRelScope = nodeRelScope_;
    std::vector<std::unique_ptr<BoundCreateInfo>> result;
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection.getQueryGraph(i);
        for (auto j = 0u; j < queryGraph->getNumQueryNodes(); ++j) {
            auto node = queryGraph->getQueryNode(j);
            if (nodeRelScope.contains(node)) {
                continue;
            }
            nodeRelScope.insert(node);
            result.push_back(bindCreateNodeInfo(node, keyValCollection));
        }
        for (auto j = 0u; j < queryGraph->getNumQueryRels(); ++j) {
            auto rel = queryGraph->getQueryRel(j);
            if (nodeRelScope.contains(rel)) {
                continue;
            }
            nodeRelScope.insert(rel);
            result.push_back(bindCreateRelInfo(rel, keyValCollection));
        }
    }
    if (result.empty()) {
        throw BinderException("Cannot resolve any node or relationship to create.");
    }
    return result;
}

std::unique_ptr<BoundCreateInfo> Binder::bindCreateNodeInfo(
    std::shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Create node " + node->toString() + " with multiple node labels is not supported.");
    }
    auto nodeTableID = node->getSingleTableID();
    auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(
        catalog.getReadOnlyVersion()->getTableSchema(nodeTableID));
    auto primaryKey = nodeTableSchema->getPrimaryKey();
    std::shared_ptr<Expression> primaryKeyExpression;
    std::vector<expression_pair> setItems;
    for (auto& property : catalog.getReadOnlyVersion()->getProperties(nodeTableID)) {
        if (collection.hasKeyVal(node, property->getName())) {
            setItems.emplace_back(collection.getKeyVal(node, property->getName()));
        } else {
            auto propertyExpression =
                expressionBinder.bindNodePropertyExpression(*node, property->getName());
            auto nullExpression = expressionBinder.createNullLiteralExpression();
            nullExpression = ExpressionBinder::implicitCastIfNecessary(
                nullExpression, propertyExpression->dataType);
            setItems.emplace_back(std::move(propertyExpression), std::move(nullExpression));
        }
    }
    for (auto& [key, val] : collection.getKeyVals(node)) {
        auto propertyExpression = static_pointer_cast<PropertyExpression>(key);
        if (propertyExpression->getPropertyID(nodeTableID) == primaryKey->getPropertyID()) {
            primaryKeyExpression = val;
        }
    }
    if (primaryKey->getDataType()->getLogicalTypeID() != LogicalTypeID::SERIAL &&
        primaryKeyExpression == nullptr) {
        throw BinderException("Create node " + node->toString() + " expects primary key " +
                              primaryKey->getName() + " as input.");
    }
    return std::make_unique<BoundCreateInfo>(
        UpdateTableType::NODE, std::move(node), std::move(setItems));
}

std::unique_ptr<BoundCreateInfo> Binder::bindCreateRelInfo(
    std::shared_ptr<RelExpression> rel, const PropertyKeyValCollection& collection) {
    if (rel->isMultiLabeled() || rel->isBoundByMultiLabeledNode()) {
        throw BinderException(
            "Create rel " + rel->toString() +
            " with multiple rel labels or bound by multiple node labels is not supported.");
    }
    auto relTableID = rel->getSingleTableID();
    auto catalogContent = catalog.getReadOnlyVersion();
    // CreateRel requires all properties in schema as input. So we rewrite set property to
    // null if user does not specify a property in the query.
    std::vector<expression_pair> setItems;
    for (auto& property : catalogContent->getProperties(relTableID)) {
        if (collection.hasKeyVal(rel, property->getName())) {
            setItems.push_back(collection.getKeyVal(rel, property->getName()));
        } else {
            auto propertyExpression =
                expressionBinder.bindRelPropertyExpression(*rel, property->getName());
            auto nullExpression = expressionBinder.createNullLiteralExpression();
            nullExpression = ExpressionBinder::implicitCastIfNecessary(
                nullExpression, propertyExpression->dataType);
            setItems.emplace_back(std::move(propertyExpression), std::move(nullExpression));
        }
    }
    return std::make_unique<BoundCreateInfo>(
        UpdateTableType::REL, std::move(rel), std::move(setItems));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindSetClause(const UpdatingClause& updatingClause) {
    auto& setClause = (SetClause&)updatingClause;
    auto boundSetClause = std::make_unique<BoundSetClause>();
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        boundSetClause->addInfo(bindSetPropertyInfo(setClause.getSetItem(i)));
    }
    return boundSetClause;
}

std::unique_ptr<BoundSetPropertyInfo> Binder::bindSetPropertyInfo(
    std::pair<parser::ParsedExpression*, parser::ParsedExpression*> setItem) {
    auto left = expressionBinder.bindExpression(*setItem.first->getChild(0));
    switch (left->dataType.getLogicalTypeID()) {
    case LogicalTypeID::NODE: {
        return std::make_unique<BoundSetPropertyInfo>(
            UpdateTableType::NODE, left, bindSetItem(setItem));
    }
    case LogicalTypeID::REL: {
        return std::make_unique<BoundSetPropertyInfo>(
            UpdateTableType::REL, left, bindSetItem(setItem));
    }
    default:
        throw BinderException(
            "Set " + expressionTypeToString(left->expressionType) + " property is supported.");
    }
}

expression_pair Binder::bindSetItem(std::pair<ParsedExpression*, ParsedExpression*> setItem) {
    auto boundLhs = expressionBinder.bindExpression(*setItem.first);
    auto boundRhs = expressionBinder.bindExpression(*setItem.second);
    boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
    return make_pair(std::move(boundLhs), std::move(boundRhs));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindDeleteClause(
    const UpdatingClause& updatingClause) {
    auto& deleteClause = (DeleteClause&)updatingClause;
    auto boundDeleteClause = std::make_unique<BoundDeleteClause>();
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto nodeOrRel = expressionBinder.bindExpression(*deleteClause.getExpression(i));
        switch (nodeOrRel->dataType.getLogicalTypeID()) {
        case LogicalTypeID::NODE: {
            auto deleteNodeInfo =
                std::make_unique<BoundDeleteInfo>(UpdateTableType::NODE, nodeOrRel);
            boundDeleteClause->addInfo(std::move(deleteNodeInfo));
        } break;
        case LogicalTypeID::REL: {
            auto rel = (RelExpression*)nodeOrRel.get();
            if (rel->getDirectionType() == RelDirectionType::BOTH) {
                throw BinderException("Delete undirected rel is not supported.");
            }
            auto deleteRel = std::make_unique<BoundDeleteInfo>(UpdateTableType::REL, nodeOrRel);
            boundDeleteClause->addInfo(std::move(deleteRel));
        } break;
        default:
            throw BinderException("Delete " + expressionTypeToString(nodeOrRel->expressionType) +
                                  " is not supported.");
        }
    }
    return boundDeleteClause;
}

} // namespace binder
} // namespace kuzu
