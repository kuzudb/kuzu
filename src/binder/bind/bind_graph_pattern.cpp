#include <set>

#include "binder/binder.h"
#include "binder/expression/path_expression.h"
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

// Grammar ensures pattern element is always connected and thus can be bound as a query graph.
std::unique_ptr<QueryGraph> Binder::bindPatternElement(
    const PatternElement& patternElement, PropertyKeyValCollection& collection) {
    auto queryGraph = std::make_unique<QueryGraph>();
    expression_vector nodeAndRels;
    auto leftNode = bindQueryNode(*patternElement.getFirstNodePattern(), *queryGraph, collection);
    nodeAndRels.push_back(leftNode);
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto patternElementChain = patternElement.getPatternElementChain(i);
        auto rightNode =
            bindQueryNode(*patternElementChain->getNodePattern(), *queryGraph, collection);
        auto rel = bindQueryRel(
            *patternElementChain->getRelPattern(), leftNode, rightNode, *queryGraph, collection);
        nodeAndRels.push_back(rel);
        nodeAndRels.push_back(rightNode);
        leftNode = rightNode;
    }
    if (patternElement.hasPathName()) {
        auto pathName = patternElement.getPathName();
        auto pathExpression = createPathExpression(pathName, nodeAndRels);
        scope->addExpression(pathName, pathExpression);
    }
    return queryGraph;
}

static std::unique_ptr<LogicalType> getRecursiveRelLogicalType(
    const NodeExpression& node, const RelExpression& rel) {
    auto nodesType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(node.getDataType().copy()));
    auto relsType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(rel.getDataType().copy()));
    std::vector<std::unique_ptr<StructField>> recursiveRelFields;
    recursiveRelFields.push_back(
        std::make_unique<StructField>(InternalKeyword::NODES, std::move(nodesType)));
    recursiveRelFields.push_back(
        std::make_unique<StructField>(InternalKeyword::RELS, std::move(relsType)));
    return std::make_unique<LogicalType>(LogicalTypeID::RECURSIVE_REL,
        std::make_unique<StructTypeInfo>(std::move(recursiveRelFields)));
}

std::shared_ptr<Expression> Binder::createPathExpression(
    const std::string& pathName, const expression_vector& children) {
    std::unordered_set<table_id_t> nodeTableIDSet;
    std::unordered_set<table_id_t> relTableIDSet;
    for (auto& child : children) {
        switch (child->getDataType().getLogicalTypeID()) {
        case LogicalTypeID::NODE: {
            auto node = (NodeExpression*)child.get();
            for (auto tableID : node->getTableIDs()) {
                nodeTableIDSet.insert(tableID);
            }
        } break;
        case LogicalTypeID::REL: {
            auto rel = (RelExpression*)child.get();
            for (auto tableID : rel->getTableIDs()) {
                relTableIDSet.insert(tableID);
            }
        } break;
        case LogicalTypeID::RECURSIVE_REL: {
            auto recursiveRel = (RelExpression*)child.get();
            auto recursiveInfo = recursiveRel->getRecursiveInfo();
            for (auto tableID : recursiveInfo->node->getTableIDs()) {
                nodeTableIDSet.insert(tableID);
            }
            for (auto tableID : recursiveInfo->rel->getTableIDs()) {
                relTableIDSet.insert(tableID);
            }
        } break;
        default:
            throw NotImplementedException("Binder::createPathExpression");
        }
    }
    auto nodeTableIDs = std::vector<table_id_t>{nodeTableIDSet.begin(), nodeTableIDSet.end()};
    std::sort(nodeTableIDs.begin(), nodeTableIDs.end());
    auto relTableIDs = std::vector<table_id_t>{relTableIDSet.begin(), relTableIDSet.end()};
    std::sort(relTableIDs.begin(), relTableIDs.end());
    auto node = createQueryNode(InternalKeyword::ANONYMOUS, nodeTableIDs);
    auto rel = createNonRecursiveQueryRel(
        InternalKeyword::ANONYMOUS, relTableIDs, nullptr, nullptr, RelDirectionType::UNKNOWN);
    auto dataType = getRecursiveRelLogicalType(*node, *rel);
    auto uniqueName = getUniqueExpressionName(pathName);
    return std::make_shared<PathExpression>(
        *dataType, uniqueName, pathName, std::move(node), std::move(rel), children);
}

static std::vector<table_id_t> pruneRelTableIDs(const Catalog& catalog_,
    const std::vector<table_id_t>& relTableIDs, const NodeExpression& srcNode,
    const NodeExpression& dstNode) {
    auto srcNodeTableIDs = srcNode.getTableIDsSet();
    auto dstNodeTableIDs = dstNode.getTableIDsSet();
    std::vector<table_id_t> result;
    for (auto& relTableID : relTableIDs) {
        auto relTableSchema = catalog_.getReadOnlyVersion()->getRelTableSchema(relTableID);
        if (!srcNodeTableIDs.contains(relTableSchema->getSrcTableID()) ||
            !dstNodeTableIDs.contains(relTableSchema->getDstTableID())) {
            continue;
        }
        result.push_back(relTableID);
    }
    return result;
}

static std::vector<std::pair<std::string, std::vector<Property*>>> getPropertyNameAndSchemasPairs(
    const std::vector<std::string>& propertyNames,
    std::unordered_map<std::string, std::vector<Property*>> propertyNamesToSchemas) {
    std::vector<std::pair<std::string, std::vector<Property*>>> propertyNameAndSchemasPairs;
    for (auto& propertyName : propertyNames) {
        auto propertySchemas = propertyNamesToSchemas.at(propertyName);
        propertyNameAndSchemasPairs.emplace_back(propertyName, std::move(propertySchemas));
    }
    return propertyNameAndSchemasPairs;
}

static std::vector<std::pair<std::string, std::vector<Property*>>>
getRelPropertyNameAndPropertiesPairs(const std::vector<RelTableSchema*>& relTableSchemas) {
    std::vector<std::string> propertyNames; // preserve order as specified in catalog.
    std::unordered_map<std::string, std::vector<Property*>> propertyNamesToSchemas;
    for (auto& relTableSchema : relTableSchemas) {
        for (auto& property : relTableSchema->properties) {
            auto propertyName = property->getName();
            if (!propertyNamesToSchemas.contains(propertyName)) {
                propertyNames.push_back(propertyName);
                propertyNamesToSchemas.emplace(propertyName, std::vector<Property*>{});
            }
            propertyNamesToSchemas.at(propertyName).push_back(property.get());
        }
    }
    return getPropertyNameAndSchemasPairs(propertyNames, propertyNamesToSchemas);
}

static std::vector<std::pair<std::string, std::vector<Property*>>>
getNodePropertyNameAndPropertiesPairs(const std::vector<NodeTableSchema*>& nodeTableSchemas) {
    std::vector<std::string> propertyNames; // preserve order as specified in catalog.
    std::unordered_map<std::string, std::vector<Property*>> propertyNamesToSchemas;
    for (auto& nodeTableSchema : nodeTableSchemas) {
        for (auto& property : nodeTableSchema->properties) {
            auto propertyName = property->getName();
            if (!propertyNamesToSchemas.contains(propertyName)) {
                propertyNames.push_back(propertyName);
                propertyNamesToSchemas.emplace(propertyName, std::vector<Property*>{});
            }
            propertyNamesToSchemas.at(propertyName).push_back(property.get());
        }
    }
    return getPropertyNameAndSchemasPairs(propertyNames, propertyNamesToSchemas);
}

std::shared_ptr<RelExpression> Binder::bindQueryRel(const RelPattern& relPattern,
    const std::shared_ptr<NodeExpression>& leftNode,
    const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
    PropertyKeyValCollection& collection) {
    auto parsedName = relPattern.getVariableName();
    if (scope->contains(parsedName)) {
        auto prevVariable = scope->getExpression(parsedName);
        auto expectedDataType = QueryRelTypeUtils::isRecursive(relPattern.getRelType()) ?
                                    LogicalTypeID::RECURSIVE_REL :
                                    LogicalTypeID::REL;
        ExpressionBinder::validateExpectedDataType(*prevVariable, expectedDataType);
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
        throw NotImplementedException("Binder::bindQueryRel");
    }
    // bind variable length
    std::shared_ptr<RelExpression> queryRel;
    if (QueryRelTypeUtils::isRecursive(relPattern.getRelType())) {
        queryRel = createRecursiveQueryRel(relPattern, tableIDs, srcNode, dstNode, directionType);
    } else {
        if (directionType == RelDirectionType::SINGLE) {
            // We perform table ID pruning as an optimization. BOTH direction type requires a more
            // advanced pruning logic because it does not have notion of src & dst by nature.
            tableIDs = pruneRelTableIDs(catalog, tableIDs, *srcNode, *dstNode);
        }
        if (tableIDs.empty()) {
            throw BinderException("Nodes " + srcNode->toString() + " and " + dstNode->toString() +
                                  " are not connected through rel " + parsedName + ".");
        }
        queryRel = createNonRecursiveQueryRel(
            relPattern.getVariableName(), tableIDs, srcNode, dstNode, directionType);
        for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
            auto boundLhs = expressionBinder.bindRelPropertyExpression(*queryRel, propertyName);
            auto boundRhs = expressionBinder.bindExpression(*rhs);
            boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
            collection.addKeyVal(queryRel, propertyName, std::make_pair(boundLhs, boundRhs));
        }
    }
    queryRel->setAlias(parsedName);
    if (!parsedName.empty()) {
        scope->addExpression(parsedName, queryRel);
    }
    queryGraph.addQueryRel(queryRel);
    return queryRel;
}

std::shared_ptr<RelExpression> Binder::createNonRecursiveQueryRel(const std::string& parsedName,
    const std::vector<table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    auto queryRel = make_shared<RelExpression>(LogicalType(LogicalTypeID::REL),
        getUniqueExpressionName(parsedName), parsedName, tableIDs, std::move(srcNode),
        std::move(dstNode), directionType, QueryRelType::NON_RECURSIVE);
    queryRel->setAlias(parsedName);
    bindQueryRelProperties(*queryRel);
    queryRel->setLabelExpression(expressionBinder.bindLabelFunction(*queryRel));
    std::vector<std::unique_ptr<StructField>> relFields;
    relFields.push_back(std::make_unique<StructField>(
        InternalKeyword::SRC, std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID)));
    relFields.push_back(std::make_unique<StructField>(
        InternalKeyword::DST, std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID)));
    relFields.push_back(std::make_unique<StructField>(
        InternalKeyword::LABEL, std::make_unique<LogicalType>(LogicalTypeID::STRING)));
    for (auto& expression : queryRel->getPropertyExpressions()) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        relFields.push_back(std::make_unique<StructField>(
            propertyExpression->getPropertyName(), propertyExpression->getDataType().copy()));
    }
    RelType::setExtraTypeInfo(
        queryRel->getDataTypeReference(), std::make_unique<StructTypeInfo>(std::move(relFields)));
    return queryRel;
}

std::shared_ptr<RelExpression> Binder::createRecursiveQueryRel(const parser::RelPattern& relPattern,
    const std::vector<table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    std::unordered_set<table_id_t> recursiveNodeTableIDs;
    for (auto relTableID : tableIDs) {
        auto relTableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(relTableID);
        recursiveNodeTableIDs.insert(relTableSchema->getSrcTableID());
        recursiveNodeTableIDs.insert(relTableSchema->getDstTableID());
    }
    auto recursiveRelPatternInfo = relPattern.getRecursiveInfo();
    auto tmpNode = createQueryNode(InternalKeyword::ANONYMOUS,
        std::vector<table_id_t>{recursiveNodeTableIDs.begin(), recursiveNodeTableIDs.end()});
    auto tmpNodeCopy = createQueryNode(InternalKeyword::ANONYMOUS,
        std::vector<table_id_t>{recursiveNodeTableIDs.begin(), recursiveNodeTableIDs.end()});
    auto prevScope = saveScope();
    scope->clear();
    auto tmpRel = createNonRecursiveQueryRel(
        recursiveRelPatternInfo->relName, tableIDs, nullptr, nullptr, directionType);
    scope->addExpression(tmpRel->toString(), tmpRel);
    expression_vector predicates;
    for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
        auto boundLhs = expressionBinder.bindRelPropertyExpression(*tmpRel, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        predicates.push_back(
            expressionBinder.createEqualityComparisonExpression(boundLhs, boundRhs));
    }
    if (recursiveRelPatternInfo->whereExpression != nullptr) {
        predicates.push_back(
            expressionBinder.bindExpression(*recursiveRelPatternInfo->whereExpression));
    }
    restoreScope(std::move(prevScope));
    auto parsedName = relPattern.getVariableName();
    auto queryRel = make_shared<RelExpression>(*getRecursiveRelLogicalType(*tmpNode, *tmpRel),
        getUniqueExpressionName(parsedName), parsedName, tableIDs, std::move(srcNode),
        std::move(dstNode), directionType, relPattern.getRelType());
    auto lengthExpression = expressionBinder.createInternalLengthExpression(*queryRel);
    auto [lowerBound, upperBound] = bindVariableLengthRelBound(relPattern);
    auto recursiveInfo = std::make_unique<RecursiveInfo>(lowerBound, upperBound, std::move(tmpNode),
        std::move(tmpNodeCopy), std::move(tmpRel), std::move(lengthExpression),
        std::move(predicates));
    queryRel->setRecursiveInfo(std::move(recursiveInfo));
    bindQueryRelProperties(*queryRel);
    return queryRel;
}

std::pair<uint64_t, uint64_t> Binder::bindVariableLengthRelBound(
    const kuzu::parser::RelPattern& relPattern) {
    auto recursiveInfo = relPattern.getRecursiveInfo();
    auto lowerBound = std::min(TypeUtils::convertToUint32(recursiveInfo->lowerBound.c_str()),
        clientContext->varLengthExtendMaxDepth);
    auto upperBound = std::min(TypeUtils::convertToUint32(recursiveInfo->upperBound.c_str()),
        clientContext->varLengthExtendMaxDepth);
    if (lowerBound == 0 || upperBound == 0) {
        throw BinderException("Lower and upper bound of a rel must be greater than 0.");
    }
    if (lowerBound > upperBound) {
        throw BinderException(
            "Lower bound of rel " + relPattern.getVariableName() + " is greater than upperBound.");
    }
    if ((relPattern.getRelType() == QueryRelType::ALL_SHORTEST ||
            relPattern.getRelType() == QueryRelType::SHORTEST) &&
        lowerBound != 1) {
        throw BinderException("Lower bound of shortest/all_shortest path must be 1.");
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
    if (scope->contains(parsedName)) { // bind to node in scope
        auto prevVariable = scope->getExpression(parsedName);
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
        if (!parsedName.empty()) {
            scope->addExpression(parsedName, queryNode);
        }
    }
    for (auto& [propertyName, rhs] : nodePattern.getPropertyKeyVals()) {
        auto boundLhs = expressionBinder.bindNodePropertyExpression(*queryNode, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        collection.addKeyVal(queryNode, propertyName, std::make_pair(boundLhs, boundRhs));
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
    const std::string& parsedName, const std::vector<table_id_t>& tableIDs) {
    auto queryNode = make_shared<NodeExpression>(LogicalType(LogicalTypeID::NODE),
        getUniqueExpressionName(parsedName), parsedName, tableIDs);
    queryNode->setAlias(parsedName);
    bindQueryNodeProperties(*queryNode);
    queryNode->setInternalIDProperty(expressionBinder.createInternalNodeIDExpression(*queryNode));
    queryNode->setLabelExpression(expressionBinder.bindLabelFunction(*queryNode));
    std::vector<std::unique_ptr<StructField>> nodeFields;
    nodeFields.push_back(std::make_unique<StructField>(
        InternalKeyword::ID, std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID)));
    nodeFields.push_back(std::make_unique<StructField>(
        InternalKeyword::LABEL, std::make_unique<LogicalType>(LogicalTypeID::STRING)));
    for (auto& expression : queryNode->getPropertyExpressions()) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        nodeFields.push_back(std::make_unique<StructField>(
            propertyExpression->getPropertyName(), propertyExpression->getDataType().copy()));
    }
    NodeType::setExtraTypeInfo(
        queryNode->getDataTypeReference(), std::make_unique<StructTypeInfo>(std::move(nodeFields)));
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
            isPrimaryKey = tableSchemas[0]->getPrimaryKey()->getPropertyID() ==
                           propertySchemas[0]->getPropertyID();
        }
        auto propertyExpression =
            expressionBinder.createPropertyExpression(node, propertySchemas, isPrimaryKey);
        node.addPropertyExpression(propertyName, std::move(propertyExpression));
    }
}

std::vector<table_id_t> Binder::bindNodeTableIDs(const std::vector<std::string>& tableNames) {
    if (catalog.getReadOnlyVersion()->getNodeTableIDs().empty()) {
        throw BinderException("No node table exists in database.");
    }
    std::unordered_set<table_id_t> tableIDs;
    if (tableNames.empty()) {
        for (auto tableID : catalog.getReadOnlyVersion()->getNodeTableIDs()) {
            tableIDs.insert(tableID);
        }
    } else {
        for (auto& tableName : tableNames) {
            tableIDs.insert(bindNodeTableID(tableName));
        }
    }
    auto result = std::vector<table_id_t>{tableIDs.begin(), tableIDs.end()};
    std::sort(result.begin(), result.end());
    return result;
}

std::vector<table_id_t> Binder::bindRelTableIDs(const std::vector<std::string>& tableNames) {
    if (catalog.getReadOnlyVersion()->getRelTableIDs().empty()) {
        throw BinderException("No rel table exists in database.");
    }
    std::unordered_set<table_id_t> tableIDs;
    if (tableNames.empty()) {
        for (auto tableID : catalog.getReadOnlyVersion()->getRelTableIDs()) {
            tableIDs.insert(tableID);
        }
    }
    for (auto& tableName : tableNames) {
        tableIDs.insert(bindRelTableID(tableName));
    }
    auto result = std::vector<table_id_t>{tableIDs.begin(), tableIDs.end()};
    std::sort(result.begin(), result.end());
    return result;
}

} // namespace binder
} // namespace kuzu
