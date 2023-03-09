#include <set>

#include "binder/binder.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

// A graph pattern contains node/rel and a set of key-value pairs associated with the variable. We
// bind node/rel as query graph and key-value pairs as a separate collection. This collection is
// interpreted in two different ways.
//    - In MATCH clause, these are additional predicates to WHERE clause
//    - In UPDATE clause, there are properties to set.
// We do not store key-value pairs in query graph primarily because we will merge key-value
// std::pairs with other predicates specified in WHERE clause.
std::pair<std::unique_ptr<QueryGraphCollection>, std::unique_ptr<PropertyKeyValCollection>>
Binder::bindGraphPattern(const std::vector<std::unique_ptr<PatternElement>>& graphPattern) {
    auto propertyCollection = std::make_unique<PropertyKeyValCollection>();
    auto queryGraphCollection = std::make_unique<QueryGraphCollection>();
    for (auto& patternElement : graphPattern) {
        queryGraphCollection->addAndMergeQueryGraphIfConnected(
            bindPatternElement(*patternElement, *propertyCollection));
    }
    return std::make_pair(std::move(queryGraphCollection), std::move(propertyCollection));
}

// Grammar ensures pattern element is always connected and thus can be bound as a query graph.
std::unique_ptr<QueryGraph> Binder::bindPatternElement(
    const PatternElement& patternElement, PropertyKeyValCollection& collection) {
    auto queryGraph = std::make_unique<QueryGraph>();
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

static std::vector<table_id_t> pruneRelTableIDs(const Catalog& catalog_,
    const std::vector<table_id_t>& relTableIDs, const NodeExpression& srcNode,
    const NodeExpression& dstNode) {
    auto srcNodeTableIDs = srcNode.getTableIDsSet();
    auto dstNodeTableIDs = dstNode.getTableIDsSet();
    std::vector<table_id_t> result;
    for (auto& relTableID : relTableIDs) {
        auto relTableSchema = catalog_.getReadOnlyVersion()->getRelTableSchema(relTableID);
        if (!srcNodeTableIDs.contains(relTableSchema->srcTableID) ||
            !dstNodeTableIDs.contains(relTableSchema->dstTableID)) {
            continue;
        }
        result.push_back(relTableID);
    }
    return result;
}

static std::vector<std::pair<std::string, std::vector<Property>>> getPropertyNameAndSchemasPairs(
    const std::vector<std::string>& propertyNames,
    std::unordered_map<std::string, std::vector<Property>> propertyNamesToSchemas) {
    std::vector<std::pair<std::string, std::vector<Property>>> propertyNameAndSchemasPairs;
    for (auto& propertyName : propertyNames) {
        auto propertySchemas = propertyNamesToSchemas.at(propertyName);
        propertyNameAndSchemasPairs.emplace_back(propertyName, std::move(propertySchemas));
    }
    return propertyNameAndSchemasPairs;
}

static std::vector<std::pair<std::string, std::vector<Property>>>
getRelPropertyNameAndPropertiesPairs(const std::vector<RelTableSchema*>& relTableSchemas) {
    std::vector<std::string> propertyNames; // preserve order as specified in catalog.
    std::unordered_map<std::string, std::vector<Property>> propertyNamesToSchemas;
    for (auto& relTableSchema : relTableSchemas) {
        for (auto& property : relTableSchema->properties) {
            if (!propertyNamesToSchemas.contains(property.name)) {
                propertyNames.push_back(property.name);
                propertyNamesToSchemas.insert({property.name, std::vector<Property>{}});
            }
            propertyNamesToSchemas.at(property.name).push_back(property);
        }
    }
    return getPropertyNameAndSchemasPairs(propertyNames, propertyNamesToSchemas);
}

static std::vector<std::pair<std::string, std::vector<Property>>>
getNodePropertyNameAndPropertiesPairs(const std::vector<NodeTableSchema*>& nodeTableSchemas) {
    std::vector<std::string> propertyNames; // preserve order as specified in catalog.
    std::unordered_map<std::string, std::vector<Property>> propertyNamesToSchemas;
    for (auto& nodeTableSchema : nodeTableSchemas) {
        for (auto& property : nodeTableSchema->properties) {
            if (!propertyNamesToSchemas.contains(property.name)) {
                propertyNames.push_back(property.name);
                propertyNamesToSchemas.insert({property.name, std::vector<Property>{}});
            }
            propertyNamesToSchemas.at(property.name).push_back(property);
        }
    }
    return getPropertyNameAndSchemasPairs(propertyNames, propertyNamesToSchemas);
}

void Binder::bindQueryRel(const RelPattern& relPattern,
    const std::shared_ptr<NodeExpression>& leftNode,
    const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
    PropertyKeyValCollection& collection) {
    auto parsedName = relPattern.getVariableName();
    if (variablesInScope.contains(parsedName)) {
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, REL);
        throw BinderException("Bind relationship " + parsedName +
                              " to relationship with same name is not supported.");
    }
    auto tableIDs = bindRelTableIDs(relPattern.getTableNames());
    // bind node to rel
    auto isLeftNodeSrc = RIGHT == relPattern.getDirection();
    auto srcNode = isLeftNodeSrc ? leftNode : rightNode;
    auto dstNode = isLeftNodeSrc ? rightNode : leftNode;
    if (srcNode->getUniqueName() == dstNode->getUniqueName()) {
        throw BinderException("Self-loop rel " + parsedName + " is not supported.");
    }
    // prune rel table IDs
    tableIDs = pruneRelTableIDs(catalog, tableIDs, *srcNode, *dstNode);
    if (tableIDs.empty()) {
        throw BinderException("Nodes " + srcNode->toString() + " and " + dstNode->toString() +
                              " are not connected through rel " + parsedName + ".");
    }
    // bind variable length
    auto [lowerBound, upperBound] = bindVariableLengthRelBound(relPattern);
    auto queryRel = make_shared<RelExpression>(getUniqueExpressionName(parsedName), parsedName,
        tableIDs, srcNode, dstNode, lowerBound, upperBound);
    queryRel->setAlias(parsedName);
    // resolve properties associate with rel table
    std::vector<RelTableSchema*> relTableSchemas;
    for (auto tableID : tableIDs) {
        relTableSchemas.push_back(catalog.getReadOnlyVersion()->getRelTableSchema(tableID));
    }
    // we don't support reading property for variable length rel yet.
    if (!queryRel->isVariableLength()) {
        for (auto& [propertyName, propertySchemas] :
            getRelPropertyNameAndPropertiesPairs(relTableSchemas)) {
            auto propertyExpression = expressionBinder.createPropertyExpression(
                *queryRel, propertySchemas, false /* isPrimaryKey */);
            queryRel->addPropertyExpression(propertyName, std::move(propertyExpression));
        }
    }
    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryRel});
    }
    for (auto i = 0u; i < relPattern.getNumPropertyKeyValPairs(); ++i) {
        auto [propertyName, rhs] = relPattern.getProperty(i);
        auto boundLhs = expressionBinder.bindRelPropertyExpression(*queryRel, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        collection.addPropertyKeyValPair(*queryRel, std::make_pair(boundLhs, boundRhs));
    }
    queryGraph.addQueryRel(queryRel);
}

std::pair<uint64_t, uint64_t> Binder::bindVariableLengthRelBound(
    const kuzu::parser::RelPattern& relPattern) {
    auto lowerBound = std::min(TypeUtils::convertToUint32(relPattern.getLowerBound().c_str()),
        VAR_LENGTH_EXTEND_MAX_DEPTH);
    auto upperBound = std::min(TypeUtils::convertToUint32(relPattern.getUpperBound().c_str()),
        VAR_LENGTH_EXTEND_MAX_DEPTH);
    if (lowerBound == 0 || upperBound == 0) {
        throw BinderException("Lower and upper bound of a rel must be greater than 0.");
    }
    if (lowerBound > upperBound) {
        throw BinderException(
            "Lower bound of rel " + relPattern.getVariableName() + " is greater than upperBound.");
    }
    return std::make_pair(lowerBound, upperBound);
}

std::shared_ptr<NodeExpression> Binder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph, PropertyKeyValCollection& collection) {
    auto parsedName = nodePattern.getVariableName();
    std::shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.contains(parsedName)) { // bind to node in scope
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, NODE);
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        // E.g. MATCH (a:person) MATCH (a:organisation)
        // We bind to single node a with both labels
        if (!nodePattern.getTableNames().empty()) {
            auto otherTableIDs = bindNodeTableIDs(nodePattern.getTableNames());
            queryNode->addTableIDs(otherTableIDs);
        }
    } else {
        queryNode = createQueryNode(nodePattern);
    }
    for (auto i = 0u; i < nodePattern.getNumPropertyKeyValPairs(); ++i) {
        auto [propertyName, rhs] = nodePattern.getProperty(i);
        auto boundLhs = expressionBinder.bindNodePropertyExpression(*queryNode, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        collection.addPropertyKeyValPair(*queryNode, std::make_pair(boundLhs, boundRhs));
    }
    queryGraph.addQueryNode(queryNode);
    return queryNode;
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(const NodePattern& nodePattern) {
    auto parsedName = nodePattern.getVariableName();
    auto tableIDs = bindNodeTableIDs(nodePattern.getTableNames());
    auto queryNode =
        make_shared<NodeExpression>(getUniqueExpressionName(parsedName), parsedName, tableIDs);
    queryNode->setAlias(parsedName);
    queryNode->setInternalIDProperty(expressionBinder.createInternalNodeIDExpression(*queryNode));
    // resolve properties associate with node table
    std::vector<NodeTableSchema*> nodeTableSchemas;
    for (auto tableID : tableIDs) {
        nodeTableSchemas.push_back(catalog.getReadOnlyVersion()->getNodeTableSchema(tableID));
    }
    auto isSingleTable = nodeTableSchemas.size() == 1;
    for (auto& [propertyName, propertySchemas] :
        getNodePropertyNameAndPropertiesPairs(nodeTableSchemas)) {
        auto isPrimaryKey = isSingleTable && nodeTableSchemas[0]->getPrimaryKey().propertyID ==
                                                 propertySchemas[0].propertyID;
        auto propertyExpression =
            expressionBinder.createPropertyExpression(*queryNode, propertySchemas, isPrimaryKey);
        queryNode->addPropertyExpression(propertyName, std::move(propertyExpression));
    }
    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryNode});
    }
    return queryNode;
}

std::vector<table_id_t> Binder::bindTableIDs(
    const std::vector<std::string>& tableNames, DataTypeID nodeOrRelType) {
    std::unordered_set<table_id_t> tableIDs;
    switch (nodeOrRelType) {
    case NODE: {
        if (tableNames.empty()) {
            for (auto tableID : catalog.getReadOnlyVersion()->getNodeTableIDs()) {
                tableIDs.insert(tableID);
            }
        } else {
            for (auto& tableName : tableNames) {
                tableIDs.insert(bindNodeTableID(tableName));
            }
        }

    } break;
    case REL: {
        if (tableNames.empty()) {
            for (auto tableID : catalog.getReadOnlyVersion()->getRelTableIDs()) {
                tableIDs.insert(tableID);
            }
        }
        for (auto& tableName : tableNames) {
            tableIDs.insert(bindRelTableID(tableName));
        }
    } break;
    default:
        throw NotImplementedException(
            "bindTableIDs(" + Types::dataTypeToString(nodeOrRelType) + ").");
    }
    auto result = std::vector<table_id_t>{tableIDs.begin(), tableIDs.end()};
    std::sort(result.begin(), result.end());
    return result;
}

} // namespace binder
} // namespace kuzu
