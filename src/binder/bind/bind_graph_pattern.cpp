#include <set>

#include "binder/binder.h"
#include "binder/expression/property_expression.h"

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

// For undirected pattern (A)-[R]-(B), we need to match R in both FWD and BWD direction.
// Since computation always starts from one node, we need to rewrite the node table names to be
// the union of both node table names, i.e. (A|B)-[R]-(A|B)
void static rewriteNodeTableNameForUndirectedRel(const PatternElement& patternElement) {
    auto leftNode = patternElement.getFirstNodePattern();
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto patternElementChain = patternElement.getPatternElementChain(i);
        auto rightNode = patternElementChain->getNodePattern();
        if (patternElementChain->getRelPattern()->getDirection() == ArrowDirection::BOTH) {
            std::vector<std::string> tableNameUnion = {};
            auto leftTableNames = leftNode->getTableNames();
            auto rightTableNames = rightNode->getTableNames();
            if (!leftTableNames.empty() && !rightTableNames.empty()) {
                tableNameUnion.insert(
                    tableNameUnion.end(), leftTableNames.begin(), leftTableNames.end());
                tableNameUnion.insert(
                    tableNameUnion.end(), rightTableNames.begin(), rightTableNames.end());
            }
            leftNode->setTableNames(tableNameUnion);
            rightNode->setTableNames(tableNameUnion);
        }
        leftNode = rightNode;
    }
}

// Grammar ensures pattern element is always connected and thus can be bound as a query graph.
std::unique_ptr<QueryGraph> Binder::bindPatternElement(
    const PatternElement& patternElement, PropertyKeyValCollection& collection) {
    rewriteNodeTableNameForUndirectedRel(patternElement);
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

static std::unique_ptr<LogicalType> getRecursiveRelLogicalType(
    const NodeExpression& node, const RelExpression& rel) {
    std::vector<std::unique_ptr<StructField>> nodeFields;
    nodeFields.push_back(std::make_unique<StructField>(
        InternalKeyword::ID, std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID)));
    for (auto& expression : node.getPropertyExpressions()) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        nodeFields.push_back(std::make_unique<StructField>(
            propertyExpression->getPropertyName(), propertyExpression->getDataType().copy()));
    }
    auto nodeType = std::make_unique<LogicalType>(
        LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(nodeFields)));
    auto nodesType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(std::move(nodeType)));
    std::vector<std::unique_ptr<StructField>> relFields;
    for (auto& expression : rel.getPropertyExpressions()) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        relFields.push_back(std::make_unique<StructField>(
            propertyExpression->getPropertyName(), propertyExpression->getDataType().copy()));
    }
    auto relType = std::make_unique<LogicalType>(
        LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(relFields)));
    auto relsType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(std::move(relType)));
    std::vector<std::unique_ptr<StructField>> recursiveRelFields;
    recursiveRelFields.push_back(
        std::make_unique<StructField>(InternalKeyword::NODES, std::move(nodesType)));
    recursiveRelFields.push_back(
        std::make_unique<StructField>(InternalKeyword::RELS, std::move(relsType)));
    return std::make_unique<LogicalType>(LogicalTypeID::RECURSIVE_REL,
        std::make_unique<StructTypeInfo>(std::move(recursiveRelFields)));
}

void Binder::bindQueryRel(const RelPattern& relPattern,
    const std::shared_ptr<NodeExpression>& leftNode,
    const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
    PropertyKeyValCollection& collection) {
    auto parsedName = relPattern.getVariableName();
    if (variableScope->contains(parsedName)) {
        auto prevVariable = variableScope->getExpression(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, LogicalTypeID::REL);
        throw BinderException("Bind relationship " + parsedName +
                              " to relationship with same name is not supported.");
    }
    auto tableIDs = bindRelTableIDs(relPattern.getTableNames());
    // bind src & dst node
    RelDirectionType directionType;
    std::shared_ptr<NodeExpression> srcNode;
    std::shared_ptr<NodeExpression> dstNode;
    switch (relPattern.getDirection()) {
    case ArrowDirection::LEFT: {
        srcNode = rightNode;
        dstNode = leftNode;
        directionType = RelDirectionType::SINGLE;
    } break;
    case ArrowDirection::RIGHT: {
        srcNode = leftNode;
        dstNode = rightNode;
        directionType = RelDirectionType::SINGLE;
    } break;
    case ArrowDirection::BOTH: {
        // For both direction, left and right will be written with the same label set. So either one
        // being src will be correct.
        srcNode = leftNode;
        dstNode = rightNode;
        directionType = RelDirectionType::BOTH;
    } break;
    default:
        throw common::NotImplementedException("Binder::bindQueryRel");
    }
    if (srcNode->getUniqueName() == dstNode->getUniqueName()) {
        throw BinderException("Self-loop rel " + parsedName + " is not supported.");
    }
    // bind variable length
    std::shared_ptr<RelExpression> queryRel;
    if (QueryRelTypeUtils::isRecursive(relPattern.getRelType())) {
        auto [lowerBound, upperBound] = bindVariableLengthRelBound(relPattern);
        queryRel = createRecursiveQueryRel(relPattern.getVariableName(), relPattern.getRelType(),
            lowerBound, upperBound, tableIDs, srcNode, dstNode, directionType);
    } else {
        tableIDs = pruneRelTableIDs(catalog, tableIDs, *srcNode, *dstNode);
        if (tableIDs.empty()) {
            throw BinderException("Nodes " + srcNode->toString() + " and " + dstNode->toString() +
                                  " are not connected through rel " + parsedName + ".");
        }
        queryRel = createNonRecursiveQueryRel(
            relPattern.getVariableName(), tableIDs, srcNode, dstNode, directionType);
    }
    queryRel->setAlias(parsedName);
    if (!parsedName.empty()) {
        variableScope->addExpression(parsedName, queryRel);
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

std::shared_ptr<RelExpression> Binder::createNonRecursiveQueryRel(const std::string& parsedName,
    const std::vector<common::table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    auto queryRel = make_shared<RelExpression>(LogicalType(LogicalTypeID::REL),
        getUniqueExpressionName(parsedName), parsedName, tableIDs, std::move(srcNode),
        std::move(dstNode), directionType, QueryRelType::NON_RECURSIVE);
    bindQueryRelProperties(*queryRel);
    return queryRel;
}

std::shared_ptr<RelExpression> Binder::createRecursiveQueryRel(const std::string& parsedName,
    common::QueryRelType relType, uint32_t lowerBound, uint32_t upperBound,
    const std::vector<common::table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    std::unordered_set<common::table_id_t> recursiveNodeTableIDs;
    for (auto relTableID : tableIDs) {
        auto relTableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(relTableID);
        recursiveNodeTableIDs.insert(relTableSchema->srcTableID);
        recursiveNodeTableIDs.insert(relTableSchema->dstTableID);
    }
    auto tmpNode = createQueryNode(
        InternalKeyword::ANONYMOUS, std::vector<common::table_id_t>{recursiveNodeTableIDs.begin(),
                                        recursiveNodeTableIDs.end()});
    auto tmpRel = createNonRecursiveQueryRel(
        InternalKeyword::ANONYMOUS, tableIDs, nullptr, nullptr, directionType);
    auto queryRel = make_shared<RelExpression>(*getRecursiveRelLogicalType(*tmpNode, *tmpRel),
        getUniqueExpressionName(parsedName), parsedName, tableIDs, std::move(srcNode),
        std::move(dstNode), directionType, relType);
    auto lengthExpression = expressionBinder.createInternalLengthExpression(*queryRel);
    auto recursiveInfo = std::make_unique<RecursiveInfo>(
        lowerBound, upperBound, std::move(tmpNode), std::move(tmpRel), std::move(lengthExpression));
    queryRel->setRecursiveInfo(std::move(recursiveInfo));
    bindQueryRelProperties(*queryRel);
    return queryRel;
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

void Binder::bindQueryRelProperties(RelExpression& rel) {
    std::vector<RelTableSchema*> tableSchemas;
    for (auto tableID : rel.getTableIDs()) {
        tableSchemas.push_back(catalog.getReadOnlyVersion()->getRelTableSchema(tableID));
    }
    for (auto& [propertyName, propertySchemas] :
        getRelPropertyNameAndPropertiesPairs(tableSchemas)) {
        auto propertyExpression = expressionBinder.createPropertyExpression(
            rel, propertySchemas, false /* isPrimaryKey */);
        rel.addPropertyExpression(propertyName, std::move(propertyExpression));
    }
}

std::shared_ptr<NodeExpression> Binder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph, PropertyKeyValCollection& collection) {
    auto parsedName = nodePattern.getVariableName();
    std::shared_ptr<NodeExpression> queryNode;
    if (variableScope->contains(parsedName)) { // bind to node in scope
        auto prevVariable = variableScope->getExpression(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, LogicalTypeID::NODE);
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
    return createQueryNode(parsedName, tableIDs);
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(
    const std::string& parsedName, const std::vector<common::table_id_t>& tableIDs) {
    auto queryNode =
        make_shared<NodeExpression>(getUniqueExpressionName(parsedName), parsedName, tableIDs);
    queryNode->setAlias(parsedName);
    queryNode->setInternalIDProperty(expressionBinder.createInternalNodeIDExpression(*queryNode));
    bindQueryNodeProperties(*queryNode);
    if (!parsedName.empty()) {
        variableScope->addExpression(parsedName, queryNode);
    }
    return queryNode;
}

void Binder::bindQueryNodeProperties(NodeExpression& node) {
    std::vector<NodeTableSchema*> tableSchemas;
    for (auto tableID : node.getTableIDs()) {
        tableSchemas.push_back(catalog.getReadOnlyVersion()->getNodeTableSchema(tableID));
    }
    for (auto& [propertyName, propertySchemas] :
        getNodePropertyNameAndPropertiesPairs(tableSchemas)) {
        bool isPrimaryKey = false;
        if (!node.isMultiLabeled()) {
            isPrimaryKey =
                tableSchemas[0]->getPrimaryKey().propertyID == propertySchemas[0].propertyID;
        }
        auto propertyExpression =
            expressionBinder.createPropertyExpression(node, propertySchemas, isPrimaryKey);
        node.addPropertyExpression(propertyName, std::move(propertyExpression));
    }
}

std::vector<table_id_t> Binder::bindTableIDs(
    const std::vector<std::string>& tableNames, LogicalTypeID nodeOrRelType) {
    std::unordered_set<table_id_t> tableIDs;
    switch (nodeOrRelType) {
    case LogicalTypeID::NODE: {
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
    case LogicalTypeID::REL: {
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
            "bindTableIDs(" + LogicalTypeUtils::dataTypeToString(nodeOrRelType) + ").");
    }
    auto result = std::vector<table_id_t>{tableIDs.begin(), tableIDs.end()};
    std::sort(result.begin(), result.end());
    return result;
}

} // namespace binder
} // namespace kuzu
