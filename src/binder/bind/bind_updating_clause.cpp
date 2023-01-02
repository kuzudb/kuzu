#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "parser/query/updating_clause/create_clause.h"
#include "parser/query/updating_clause/delete_clause.h"
#include "parser/query/updating_clause/set_clause.h"

namespace kuzu {
namespace binder {

unique_ptr<BoundUpdatingClause> Binder::bindUpdatingClause(const UpdatingClause& updatingClause) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        return bindCreateClause(updatingClause);
    }
    case ClauseType::SET: {
        return bindSetClause(updatingClause);
    }
    case ClauseType::DELETE: {
        return bindDeleteClause(updatingClause);
    }
    default:
        throw NotImplementedException("bindUpdatingClause().");
    }
}

unique_ptr<BoundUpdatingClause> Binder::bindCreateClause(const UpdatingClause& updatingClause) {
    auto& createClause = (CreateClause&)updatingClause;
    auto prevVariablesInScope = variablesInScope;
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(createClause.getPatternElements());
    auto boundCreateClause = make_unique<BoundCreateClause>();
    for (auto i = 0u; i < queryGraphCollection->getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection->getQueryGraph(i);
        for (auto j = 0u; j < queryGraph->getNumQueryNodes(); ++j) {
            auto node = queryGraph->getQueryNode(j);
            if (!prevVariablesInScope.contains(node->getRawName())) {
                boundCreateClause->addCreateNode(bindCreateNode(node, *propertyCollection));
            }
        }
        for (auto j = 0u; j < queryGraph->getNumQueryRels(); ++j) {
            auto rel = queryGraph->getQueryRel(j);
            if (!prevVariablesInScope.contains(rel->getRawName())) {
                boundCreateClause->addCreateRel(bindCreateRel(rel, *propertyCollection));
            }
        }
    }
    return boundCreateClause;
}

unique_ptr<BoundCreateNode> Binder::bindCreateNode(
    shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Create node " + node->getRawName() + " with multiple node labels is not supported.");
    }
    auto nodeTableID = node->getSingleTableID();
    auto nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(nodeTableID);
    auto primaryKey = nodeTableSchema->getPrimaryKey();
    shared_ptr<Expression> primaryKeyExpression;
    vector<expression_pair> setItems;
    for (auto& [key, val] : collection.getPropertyKeyValPairs(*node)) {
        auto propertyExpression = static_pointer_cast<PropertyExpression>(key);
        if (propertyExpression->getPropertyID(nodeTableID) == primaryKey.propertyID) {
            primaryKeyExpression = val;
        }
        setItems.emplace_back(key, val);
    }
    if (primaryKeyExpression == nullptr) {
        throw BinderException("Create node " + node->getRawName() + " expects primary key " +
                              primaryKey.name + " as input.");
    }
    return make_unique<BoundCreateNode>(
        std::move(node), std::move(primaryKeyExpression), std::move(setItems));
}

unique_ptr<BoundCreateRel> Binder::bindCreateRel(
    shared_ptr<RelExpression> rel, const PropertyKeyValCollection& collection) {
    if (rel->isMultiLabeled() || rel->isBoundByMultiLabeledNode()) {
        throw BinderException(
            "Create rel " + rel->getRawName() +
            " with multiple rel labels or bound by multiple node labels is not supported.");
    }
    auto relTableID = rel->getSingleTableID();
    auto catalogContent = catalog.getReadOnlyVersion();
    // CreateRel requires all properties in schema as input. So we rewrite set property to
    // null if user does not specify a property in the query.
    vector<expression_pair> setItems;
    for (auto& property : catalogContent->getRelProperties(relTableID)) {
        if (collection.hasPropertyKeyValPair(*rel, property.name)) {
            setItems.push_back(collection.getPropertyKeyValPair(*rel, property.name));
        } else {
            auto propertyExpression =
                expressionBinder.bindRelPropertyExpression(rel, property.name);
            shared_ptr<Expression> nullExpression =
                LiteralExpression::createNullLiteralExpression(getUniqueExpressionName("NULL"));
            nullExpression = ExpressionBinder::implicitCastIfNecessary(
                nullExpression, propertyExpression->dataType);
            setItems.emplace_back(std::move(propertyExpression), std::move(nullExpression));
        }
    }
    return make_unique<BoundCreateRel>(std::move(rel), std::move(setItems));
}

unique_ptr<BoundUpdatingClause> Binder::bindSetClause(const UpdatingClause& updatingClause) {
    auto& setClause = (SetClause&)updatingClause;
    auto boundSetClause = make_unique<BoundSetClause>();
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        auto setItem = setClause.getSetItem(i);
        auto boundLhs = expressionBinder.bindExpression(*setItem->origin);
        auto boundNodeOrRel = boundLhs->getChild(0);
        if (boundNodeOrRel->dataType.typeID != NODE) {
            throw BinderException("Set " + Types::dataTypeToString(boundNodeOrRel->dataType) +
                                  " property is supported.");
        }
        auto boundNode = static_pointer_cast<NodeExpression>(boundNodeOrRel);
        if (boundNode->isMultiLabeled()) {
            throw BinderException("Set property of node " + boundNode->getRawName() +
                                  " with multiple node labels is not supported.");
        }
        auto boundRhs = expressionBinder.bindExpression(*setItem->target);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        boundSetClause->addSetItem(make_pair(boundLhs, boundRhs));
    }
    return boundSetClause;
}

unique_ptr<BoundUpdatingClause> Binder::bindDeleteClause(const UpdatingClause& updatingClause) {
    auto& deleteClause = (DeleteClause&)updatingClause;
    auto boundDeleteClause = make_unique<BoundDeleteClause>();
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto boundExpression = expressionBinder.bindExpression(*deleteClause.getExpression(i));
        if (boundExpression->dataType.typeID == NODE) {
            auto deleteNode = bindDeleteNode(static_pointer_cast<NodeExpression>(boundExpression));
            boundDeleteClause->addDeleteNode(std::move(deleteNode));
        } else if (boundExpression->dataType.typeID == REL) {
            auto deleteRel = bindDeleteRel(static_pointer_cast<RelExpression>(boundExpression));
            boundDeleteClause->addDeleteRel(std::move(deleteRel));
        } else {
            throw BinderException("Delete " +
                                  expressionTypeToString(boundExpression->expressionType) +
                                  " is not supported.");
        }
    }
    return boundDeleteClause;
}

unique_ptr<BoundDeleteNode> Binder::bindDeleteNode(shared_ptr<NodeExpression> node) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Delete node " + node->getRawName() + " with multiple node labels is not supported.");
    }
    auto nodeTableID = node->getSingleTableID();
    auto nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(nodeTableID);
    auto primaryKey = nodeTableSchema->getPrimaryKey();
    auto primaryKeyExpression =
        expressionBinder.bindNodePropertyExpression(node, vector<Property>{primaryKey});
    return make_unique<BoundDeleteNode>(node, primaryKeyExpression);
}

shared_ptr<RelExpression> Binder::bindDeleteRel(shared_ptr<RelExpression> rel) {
    if (rel->isMultiLabeled() || rel->isBoundByMultiLabeledNode()) {
        throw BinderException(
            "Delete rel " + rel->getRawName() +
            " with multiple rel labels or bound by multiple node labels is not supported.");
    }
    return rel;
}

} // namespace binder
} // namespace kuzu
