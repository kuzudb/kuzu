#include "src/binder/expression/include/literal_expression.h"
#include "src/binder/include/binder.h"
#include "src/binder/query/updating_clause/include/bound_create_clause.h"
#include "src/binder/query/updating_clause/include/bound_delete_clause.h"
#include "src/binder/query/updating_clause/include/bound_set_clause.h"
#include "src/parser/query/updating_clause/include/create_clause.h"
#include "src/parser/query/updating_clause/include/delete_clause.h"
#include "src/parser/query/updating_clause/include/set_clause.h"

namespace graphflow {
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
        assert(false);
    }
}

unique_ptr<BoundUpdatingClause> Binder::bindCreateClause(const UpdatingClause& updatingClause) {
    auto& createClause = (CreateClause&)updatingClause;
    auto prevVariablesInScope = variablesInScope;
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(createClause.getPatternElements());
    vector<unique_ptr<BoundCreateNode>> boundCreateNodes;
    vector<unique_ptr<BoundCreateRel>> boundCreateRels;
    for (auto i = 0u; i < queryGraphCollection->getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection->getQueryGraph(i);
        for (auto j = 0u; j < queryGraph->getNumQueryNodes(); ++j) {
            auto node = queryGraph->getQueryNode(j);
            if (!prevVariablesInScope.contains(node->getRawName())) {
                boundCreateNodes.push_back(bindCreateNode(node, *propertyCollection));
            }
        }
        for (auto j = 0u; j < queryGraph->getNumQueryRels(); ++j) {
            auto rel = queryGraph->getQueryRel(j);
            if (!prevVariablesInScope.contains(rel->getRawName())) {
                boundCreateRels.push_back(bindCreateRel(rel, *propertyCollection));
            }
        }
    }
    auto boundCreateClause =
        make_unique<BoundCreateClause>(std::move(boundCreateNodes), std::move(boundCreateRels));
    return boundCreateClause;
}

unique_ptr<BoundCreateNode> Binder::bindCreateNode(
    shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection) {
    auto nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(node->getTableID());
    auto primaryKey = nodeTableSchema->getPrimaryKey();
    shared_ptr<Expression> primaryKeyExpression;
    vector<expression_pair> setItems;
    for (auto& [key, val] : collection.getPropertyKeyValPairs(*node)) {
        auto propertyExpression = static_pointer_cast<PropertyExpression>(key);
        if (propertyExpression->getPropertyID() == primaryKey.propertyID) {
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
    auto catalogContent = catalog.getReadOnlyVersion();
    // CreateRel requires all properties in schema as input. So we rewrite set property to
    // null if user does not specify a property in the query.
    vector<expression_pair> setItems;
    for (auto& property : catalogContent->getRelProperties(rel->getTableID())) {
        if (collection.hasPropertyKeyValPair(*rel, property.name)) {
            setItems.push_back(collection.getPropertyKeyValPair(*rel, property.name));
        } else {
            auto propertyExpression = make_shared<PropertyExpression>(
                property.dataType, property.name, property.propertyID, rel);
            setItems.emplace_back(std::move(propertyExpression),
                LiteralExpression::createNullLiteralExpression(property.dataType));
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
        if (boundExpression->dataType.typeID != NODE) {
            throw BinderException("Delete " +
                                  expressionTypeToString(boundExpression->expressionType) +
                                  " is not supported.");
        }
        boundDeleteClause->addExpression(move(boundExpression));
    }
    return boundDeleteClause;
}

} // namespace binder
} // namespace graphflow
