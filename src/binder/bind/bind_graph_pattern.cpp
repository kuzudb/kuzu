#include "binder/binder.h"

namespace kuzu {
namespace binder {

// A graph pattern contains node/rel and a set of key-value pairs associated with the variable. We
// bind node/rel as query graph and key-value pairs as a separate collection. This collection is
// interpreted in two different ways.
//    - In MATCH clause, these are additional predicates to WHERE clause
//    - In UPDATE clause, there are properties to set.
// We do not store key-value pairs in query graph primarily because we will merge key-value pairs
// with other predicates specified in WHERE clause.
pair<unique_ptr<QueryGraphCollection>, unique_ptr<PropertyKeyValCollection>>
Binder::bindGraphPattern(const vector<unique_ptr<PatternElement>>& graphPattern) {
    auto propertyCollection = make_unique<PropertyKeyValCollection>();
    auto queryGraphCollection = make_unique<QueryGraphCollection>();
    for (auto& patternElement : graphPattern) {
        queryGraphCollection->addAndMergeQueryGraphIfConnected(
            bindPatternElement(*patternElement, *propertyCollection));
    }
    return make_pair(std::move(queryGraphCollection), std::move(propertyCollection));
}

// Grammar ensures pattern element is always connected and thus can be bound as a query graph.
unique_ptr<QueryGraph> Binder::bindPatternElement(
    const PatternElement& patternElement, PropertyKeyValCollection& collection) {
    auto queryGraph = make_unique<QueryGraph>();
    auto leftNode = bindQueryNode(*patternElement.getFirstNodePattern(), *queryGraph, collection);
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto patternElementChain = patternElement.getPatternElementChain(i);
        auto rightNode =
            bindQueryNode(*patternElementChain->getNodePattern(), *queryGraph, collection);
        bindQueryRel(
            *patternElementChain->getRelPattern(), leftNode, rightNode, *queryGraph, collection);
        leftNode = rightNode;
    }
    return queryGraph;
}

// TODO(Xiyang): remove this validation when we support full multi-labeled query
static void validateNodeRelConnectivity(table_id_t srcTableID, table_id_t dstTableID,
    table_id_t relTableID, const CatalogContent& catalogContent) {
    for (auto& [srcTableID_, dstTableID_] :
        catalogContent.getRelTableSchema(relTableID)->srcDstTableIDs) {
        if (srcTableID_ == srcTableID && dstTableID_ == dstTableID) {
            return;
        }
    }
    throw BinderException("Node table " + catalogContent.getNodeTableName(srcTableID) +
                          " doesn't connect to " + catalogContent.getNodeTableName(dstTableID) +
                          " through rel table " + catalogContent.getRelTableName(relTableID) + ".");
}

// E.g. MATCH (:person)-[:studyAt]->(:person) ...
static void validateNodeRelConnectivity(const Catalog& catalog_, const RelExpression& rel,
    const NodeExpression& srcNode, const NodeExpression& dstNode) {
    for (auto srcTableID : srcNode.getTableIDs()) {
        for (auto dstTableID : dstNode.getTableIDs()) {
            validateNodeRelConnectivity(
                srcTableID, dstTableID, rel.getTableID(), *catalog_.getReadOnlyVersion());
        }
    }
}

void Binder::bindQueryRel(const RelPattern& relPattern, const shared_ptr<NodeExpression>& leftNode,
    const shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
    PropertyKeyValCollection& collection) {
    auto parsedName = relPattern.getVariableName();
    if (variablesInScope.contains(parsedName)) {
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, REL);
        throw BinderException("Bind relationship " + parsedName +
                              " to relationship with same name is not supported.");
    }
    auto tableID = bindRelTable(relPattern.getTableName());
    if (ANY_TABLE_ID == tableID) {
        throw BinderException(
            "Any-table is not supported. " + parsedName + " does not have a table.");
    }
    // bind node to rel
    auto isLeftNodeSrc = RIGHT == relPattern.getDirection();
    auto srcNode = isLeftNodeSrc ? leftNode : rightNode;
    auto dstNode = isLeftNodeSrc ? rightNode : leftNode;
    if (srcNode->getUniqueName() == dstNode->getUniqueName()) {
        throw BinderException("Self-loop rel " + parsedName + " is not supported.");
    }
    // bind variable length
    auto lowerBound = min(TypeUtils::convertToUint32(relPattern.getLowerBound().c_str()),
        VAR_LENGTH_EXTEND_MAX_DEPTH);
    auto upperBound = min(TypeUtils::convertToUint32(relPattern.getUpperBound().c_str()),
        VAR_LENGTH_EXTEND_MAX_DEPTH);
    if (lowerBound == 0 || upperBound == 0) {
        throw BinderException("Lower and upper bound of a rel must be greater than 0.");
    }
    if (lowerBound > upperBound) {
        throw BinderException("Lower bound of rel " + parsedName + " is greater than upperBound.");
    }
    auto queryRel = make_shared<RelExpression>(
        getUniqueExpressionName(parsedName), tableID, srcNode, dstNode, lowerBound, upperBound);
    if (!queryRel->isVariableLength()) {
        queryRel->setInternalIDProperty(expressionBinder.bindInternalIDExpression(queryRel));
    }
    queryRel->setAlias(parsedName);
    queryRel->setRawName(parsedName);
    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryRel});
    }
    validateNodeRelConnectivity(catalog, *queryRel, *srcNode, *dstNode);
    for (auto i = 0u; i < relPattern.getNumPropertyKeyValPairs(); ++i) {
        auto [propertyName, rhs] = relPattern.getProperty(i);
        auto boundLhs = expressionBinder.bindRelPropertyExpression(queryRel, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        collection.addPropertyKeyValPair(*queryRel, make_pair(boundLhs, boundRhs));
    }
    queryGraph.addQueryRel(queryRel);
}

shared_ptr<NodeExpression> Binder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph, PropertyKeyValCollection& collection) {
    auto parsedName = nodePattern.getVariableName();
    shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.contains(parsedName)) { // bind to node in scope
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, NODE);
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        auto otherTableIDs = bindNodeTableIDs(nodePattern.getTableNames());
        queryNode->addTableIDs(otherTableIDs);
    } else {
        queryNode = createQueryNode(nodePattern);
    }
    for (auto i = 0u; i < nodePattern.getNumPropertyKeyValPairs(); ++i) {
        auto [propertyName, rhs] = nodePattern.getProperty(i);
        auto boundLhs = expressionBinder.bindNodePropertyExpression(queryNode, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        collection.addPropertyKeyValPair(*queryNode, make_pair(boundLhs, boundRhs));
    }
    queryGraph.addQueryNode(queryNode);
    return queryNode;
}

shared_ptr<NodeExpression> Binder::createQueryNode(const NodePattern& nodePattern) {
    auto parsedName = nodePattern.getVariableName();
    auto tableIDs = bindNodeTableIDs(nodePattern.getTableNames());
    auto queryNode = make_shared<NodeExpression>(getUniqueExpressionName(parsedName), tableIDs);
    queryNode->setInternalIDProperty(expressionBinder.bindInternalIDExpression(queryNode));
    queryNode->setAlias(parsedName);
    queryNode->setRawName(parsedName);
    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryNode});
    }
    return queryNode;
}

unordered_set<table_id_t> Binder::bindNodeTableIDs(const vector<string>& nodeTableNames) {
    unordered_set<table_id_t> result;
    for (auto& nodeTableName : nodeTableNames) {
        auto nodeTableID = bindNodeTableID(nodeTableName);
        if (nodeTableID == ANY_TABLE_ID) {
            throw BinderException("Any-table is not supported");
        }
        result.insert(nodeTableID);
    }
    return result;
}

} // namespace binder
} // namespace kuzu
