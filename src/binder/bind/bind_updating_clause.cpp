#include "binder/binder.h"
#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "parser/query/updating_clause/create_clause.h"
#include "parser/query/updating_clause/delete_clause.h"
#include "parser/query/updating_clause/set_clause.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundUpdatingClause> Binder::bindUpdatingClause(
    const UpdatingClause& updatingClause) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        return bindCreateClause(updatingClause);
    }
    case ClauseType::SET: {
        return bindSetClause(updatingClause);
    }
    case ClauseType::DELETE_: {
        return bindDeleteClause(updatingClause);
    }
    default:
        throw NotImplementedException("bindUpdatingClause().");
    }
}

std::unique_ptr<BoundUpdatingClause> Binder::bindCreateClause(
    const UpdatingClause& updatingClause) {
    auto& createClause = (CreateClause&)updatingClause;
    auto prevVariableScope = variableScope->copy();
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(createClause.getPatternElements());
    auto boundCreateClause = std::make_unique<BoundCreateClause>();
    for (auto i = 0u; i < queryGraphCollection->getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection->getQueryGraph(i);
        for (auto j = 0u; j < queryGraph->getNumQueryNodes(); ++j) {
            auto node = queryGraph->getQueryNode(j);
            if (!prevVariableScope->contains(node->toString())) {
                boundCreateClause->addCreateNode(bindCreateNode(node, *propertyCollection));
            }
        }
        for (auto j = 0u; j < queryGraph->getNumQueryRels(); ++j) {
            auto rel = queryGraph->getQueryRel(j);
            if (!prevVariableScope->contains(rel->toString())) {
                boundCreateClause->addCreateRel(bindCreateRel(rel, *propertyCollection));
            }
        }
    }
    return boundCreateClause;
}

std::unique_ptr<BoundCreateNode> Binder::bindCreateNode(
    std::shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Create node " + node->toString() + " with multiple node labels is not supported.");
    }
    auto nodeTableID = node->getSingleTableID();
    auto nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(nodeTableID);
    auto primaryKey = nodeTableSchema->getPrimaryKey();
    std::shared_ptr<Expression> primaryKeyExpression;
    std::vector<expression_pair> setItems;
    for (auto& [key, val] : collection.getPropertyKeyValPairs(*node)) {
        auto propertyExpression = static_pointer_cast<PropertyExpression>(key);
        if (propertyExpression->getPropertyID(nodeTableID) == primaryKey.propertyID) {
            primaryKeyExpression = val;
        }
        setItems.emplace_back(key, val);
    }
    if (nodeTableSchema->getPrimaryKey().dataType.getLogicalTypeID() != LogicalTypeID::SERIAL &&
        primaryKeyExpression == nullptr) {
        throw BinderException("Create node " + node->toString() + " expects primary key " +
                              primaryKey.name + " as input.");
    }
    return std::make_unique<BoundCreateNode>(
        std::move(node), std::move(primaryKeyExpression), std::move(setItems));
}

std::unique_ptr<BoundCreateRel> Binder::bindCreateRel(
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
    for (auto& property : catalogContent->getRelProperties(relTableID)) {
        if (collection.hasPropertyKeyValPair(*rel, property.name)) {
            setItems.push_back(collection.getPropertyKeyValPair(*rel, property.name));
        } else {
            auto propertyExpression =
                expressionBinder.bindRelPropertyExpression(*rel, property.name);
            auto nullExpression = expressionBinder.createNullLiteralExpression();
            nullExpression = ExpressionBinder::implicitCastIfNecessary(
                nullExpression, propertyExpression->dataType);
            setItems.emplace_back(std::move(propertyExpression), std::move(nullExpression));
        }
    }
    return std::make_unique<BoundCreateRel>(std::move(rel), std::move(setItems));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindSetClause(const UpdatingClause& updatingClause) {
    auto& setClause = (SetClause&)updatingClause;
    auto boundSetClause = std::make_unique<BoundSetClause>();
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        auto setItem = setClause.getSetItem(i);
        auto nodeOrRel = expressionBinder.bindExpression(*setItem.first->getChild(0));
        switch (nodeOrRel->dataType.getLogicalTypeID()) {
        case LogicalTypeID::NODE: {
            auto node = static_pointer_cast<NodeExpression>(nodeOrRel);
            boundSetClause->addSetNodeProperty(bindSetNodeProperty(node, setItem));
        } break;
        case LogicalTypeID::REL: {
            auto rel = static_pointer_cast<RelExpression>(nodeOrRel);
            boundSetClause->addSetRelProperty(bindSetRelProperty(rel, setItem));
        } break;
        default:
            throw BinderException("Set " + expressionTypeToString(nodeOrRel->expressionType) +
                                  " property is supported.");
        }
    }
    return boundSetClause;
}

std::unique_ptr<BoundSetNodeProperty> Binder::bindSetNodeProperty(
    std::shared_ptr<NodeExpression> node, std::pair<ParsedExpression*, ParsedExpression*> setItem) {
    if (node->isMultiLabeled()) {
        throw BinderException("Set property of node " + node->toString() +
                              " with multiple node labels is not supported.");
    }
    return std::make_unique<BoundSetNodeProperty>(std::move(node), bindSetItem(setItem));
}

std::unique_ptr<BoundSetRelProperty> Binder::bindSetRelProperty(
    std::shared_ptr<RelExpression> rel, std::pair<ParsedExpression*, ParsedExpression*> setItem) {
    if (rel->isMultiLabeled() || rel->isBoundByMultiLabeledNode()) {
        throw BinderException("Set property of rel " + rel->toString() +
                              " with multiple rel labels or bound by multiple node labels "
                              "is not supported.");
    }
    return std::make_unique<BoundSetRelProperty>(std::move(rel), bindSetItem(setItem));
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
            auto deleteNode = bindDeleteNode(static_pointer_cast<NodeExpression>(nodeOrRel));
            boundDeleteClause->addDeleteNode(std::move(deleteNode));
        } break;
        case LogicalTypeID::REL: {
            auto deleteRel = bindDeleteRel(static_pointer_cast<RelExpression>(nodeOrRel));
            boundDeleteClause->addDeleteRel(std::move(deleteRel));
        } break;
        default:
            throw BinderException("Delete " + expressionTypeToString(nodeOrRel->expressionType) +
                                  " is not supported.");
        }
    }
    return boundDeleteClause;
}

std::unique_ptr<BoundDeleteNode> Binder::bindDeleteNode(
    const std::shared_ptr<NodeExpression>& node) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Delete node " + node->toString() + " with multiple node labels is not supported.");
    }
    auto nodeTableID = node->getSingleTableID();
    auto nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(nodeTableID);
    auto primaryKeyExpression =
        expressionBinder.bindNodePropertyExpression(*node, nodeTableSchema->getPrimaryKey().name);
    return std::make_unique<BoundDeleteNode>(node, primaryKeyExpression);
}

std::shared_ptr<RelExpression> Binder::bindDeleteRel(std::shared_ptr<RelExpression> rel) {
    if (rel->isMultiLabeled() || rel->isBoundByMultiLabeledNode()) {
        throw BinderException(
            "Delete rel " + rel->toString() +
            " with multiple rel labels or bound by multiple node labels is not supported.");
    }
    return rel;
}

} // namespace binder
} // namespace kuzu
