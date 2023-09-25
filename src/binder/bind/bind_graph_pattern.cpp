#include <set>

#include "binder/binder.h"
#include "binder/expression/path_expression.h"
#include "binder/expression/property_expression.h"
#include "catalog/node_table_schema.h"
#include "catalog/rdf_graph_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"

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
        auto pathExpression = createPatternElement(pathName, patternElement, nodeAndRels);
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

std::shared_ptr<Expression> Binder::createPatternElement(const std::string& pathName,
    const PatternElement& patternElement, const expression_vector& children) {
    std::unordered_set<table_id_t> nodePatternTableIDSet;
    std::unordered_set<table_id_t> relPatternTableIDSet;
    for (auto tableID : bindTableIDs(patternElement.getFirstNodePattern()->getTableNames(), true)) {
        nodePatternTableIDSet.insert(tableID);
    }
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto chain = patternElement.getPatternElementChain(i);
        for (auto tableID : bindTableIDs(chain->getNodePattern()->getTableNames(), true)) {
            nodePatternTableIDSet.insert(tableID);
        }
        for (auto tableID : bindTableIDs(chain->getRelPattern()->getTableNames(), false)) {
            relPatternTableIDSet.insert(tableID);
        }
    }
    auto nodePatternTableIDs =
        std::vector<table_id_t>{nodePatternTableIDSet.begin(), nodePatternTableIDSet.end()};
    std::sort(nodePatternTableIDs.begin(), nodePatternTableIDs.end());
    auto relPatternTableIDs =
        std::vector<table_id_t>{relPatternTableIDSet.begin(), relPatternTableIDSet.end()};
    std::sort(relPatternTableIDs.begin(), relPatternTableIDs.end());
    auto node = createQueryNode(InternalKeyword::ANONYMOUS, nodePatternTableIDs);
    auto rel = createNonRecursiveQueryRel(InternalKeyword::ANONYMOUS, relPatternTableIDs, nullptr,
        nullptr, RelDirectionType::UNKNOWN);
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
        auto relTableSchema = reinterpret_cast<RelTableSchema*>(
            catalog_.getReadOnlyVersion()->getTableSchema(relTableID));
        if (!srcNodeTableIDs.contains(relTableSchema->getSrcTableID()) ||
            !dstNodeTableIDs.contains(relTableSchema->getDstTableID())) {
            continue;
        }
        result.push_back(relTableID);
    }
    return result;
}

static std::vector<std::string> getPropertyNames(const std::vector<TableSchema*>& tableSchemas) {
    std::vector<std::string> result;
    std::unordered_set<std::string> propertyNamesSet;
    for (auto& tableSchema : tableSchemas) {
        for (auto& property : tableSchema->properties) {
            if (propertyNamesSet.contains(property->getName())) {
                continue;
            }
            propertyNamesSet.insert(property->getName());
            result.push_back(property->getName());
        }
    }
    return result;
}

static std::unique_ptr<Expression> createPropertyExpression(const std::string& propertyName,
    const std::string& uniqueVariableName, const std::string& rawVariableName,
    const std::vector<TableSchema*>& tableSchemas) {
    bool isPrimaryKey = false;
    if (tableSchemas.size() == 1 && tableSchemas[0]->tableType == TableType::NODE) {
        auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(tableSchemas[0]);
        isPrimaryKey = nodeTableSchema->getPrimaryKeyPropertyID() ==
                       nodeTableSchema->getPropertyID(propertyName);
    }
    std::unordered_map<common::table_id_t, common::property_id_t> tableIDToPropertyID;
    std::vector<LogicalType*> propertyDataTypes;
    for (auto& tableSchema : tableSchemas) {
        if (!tableSchema->containProperty(propertyName)) {
            continue;
        }
        auto propertyID = tableSchema->getPropertyID(propertyName);
        propertyDataTypes.push_back(tableSchema->getProperty(propertyID)->getDataType());
        tableIDToPropertyID.insert({tableSchema->getTableID(), propertyID});
    }
    for (auto type : propertyDataTypes) {
        if (*propertyDataTypes[0] != *type) {
            StringUtils::string_format("Expect same data type for property {} but find {} and {}",
                propertyName, LogicalTypeUtils::dataTypeToString(*type),
                LogicalTypeUtils::dataTypeToString(*propertyDataTypes[0]));
        }
    }
    return make_unique<PropertyExpression>(*propertyDataTypes[0], propertyName, uniqueVariableName,
        rawVariableName, tableIDToPropertyID, isPrimaryKey);
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
    auto tableIDs = bindTableIDs(relPattern.getTableNames(), false);
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
    // tableIDs can be relTableIDs or rdfGraphTableIDs.
    auto relTableIDs = getRelTableIDs(tableIDs);
    if (directionType == RelDirectionType::SINGLE && srcNode && dstNode) {
        // We perform table ID pruning as an optimization. BOTH direction type requires a more
        // advanced pruning logic because it does not have notion of src & dst by nature.
        relTableIDs = pruneRelTableIDs(catalog, relTableIDs, *srcNode, *dstNode);
    }
    if (relTableIDs.empty()) {
        throw BinderException("Nodes " + srcNode->toString() + " and " + dstNode->toString() +
                              " are not connected through rel " + parsedName + ".");
    }
    auto queryRel = make_shared<RelExpression>(LogicalType(LogicalTypeID::REL),
        getUniqueExpressionName(parsedName), parsedName, relTableIDs, std::move(srcNode),
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
    auto readVersion = catalog.getReadOnlyVersion();
    if (readVersion->getTableSchema(tableIDs[0])->getTableType() == TableType::RDF) {
        auto predicateID =
            expressionBinder.bindRelPropertyExpression(*queryRel, RDFKeyword::PREDICT_ID);
        auto resourceTableIDs = getNodeTableIDs(tableIDs);
        auto resourceTableSchemas = readVersion->getTableSchemas(resourceTableIDs);
        auto predicateIRI = createPropertyExpression(common::RDFKeyword::IRI,
            queryRel->getUniqueName(), queryRel->getVariableName(), resourceTableSchemas);
        auto rdfInfo =
            std::make_unique<RdfPredicateInfo>(std::move(resourceTableIDs), std::move(predicateID));
        queryRel->setRdfPredicateInfo(std::move(rdfInfo));
        queryRel->addPropertyExpression(common::RDFKeyword::IRI, std::move(predicateIRI));
    }
    return queryRel;
}

std::shared_ptr<RelExpression> Binder::createRecursiveQueryRel(const parser::RelPattern& relPattern,
    const std::vector<table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    auto relTableIDs = getRelTableIDs(tableIDs);
    std::unordered_set<table_id_t> recursiveNodeTableIDs;
    for (auto relTableID : relTableIDs) {
        auto relTableSchema = reinterpret_cast<RelTableSchema*>(
            catalog.getReadOnlyVersion()->getTableSchema(relTableID));
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
        getUniqueExpressionName(parsedName), parsedName, relTableIDs, std::move(srcNode),
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
    auto lowerBound = TypeUtils::convertToUint32(recursiveInfo->lowerBound.c_str());
    auto upperBound = clientContext->varLengthExtendMaxDepth;
    if (!recursiveInfo->upperBound.empty()) {
        upperBound = TypeUtils::convertToUint32(recursiveInfo->upperBound.c_str());
    }
    if (lowerBound > upperBound) {
        throw BinderException(
            "Lower bound of rel " + relPattern.getVariableName() + " is greater than upperBound.");
    }
    if (upperBound > clientContext->varLengthExtendMaxDepth) {
        throw BinderException(
            "Upper bound of rel " + relPattern.getVariableName() +
            " exceeds maximum: " + std::to_string(clientContext->varLengthExtendMaxDepth) + ".");
    }
    if ((relPattern.getRelType() == QueryRelType::ALL_SHORTEST ||
            relPattern.getRelType() == QueryRelType::SHORTEST) &&
        lowerBound != 1) {
        throw BinderException("Lower bound of shortest/all_shortest path must be 1.");
    }
    return std::make_pair(lowerBound, upperBound);
}

void Binder::bindQueryRelProperties(RelExpression& rel) {
    std::vector<TableSchema*> tableSchemas;
    for (auto tableID : rel.getTableIDs()) {
        tableSchemas.push_back(catalog.getReadOnlyVersion()->getTableSchema(tableID));
    }
    auto propertyNames = getPropertyNames(tableSchemas);
    for (auto& propertyName : propertyNames) {
        rel.addPropertyExpression(
            propertyName, createPropertyExpression(propertyName, rel.getUniqueName(),
                              rel.getVariableName(), tableSchemas));
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
        // We bind to a single node with both labels
        if (!nodePattern.getTableNames().empty()) {
            auto otherTableIDs = bindTableIDs(nodePattern.getTableNames(), true);
            auto otherNodeTableIDs = getNodeTableIDs(otherTableIDs);
            queryNode->addTableIDs(otherNodeTableIDs);
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
    return createQueryNode(parsedName, bindTableIDs(nodePattern.getTableNames(), true));
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(
    const std::string& parsedName, const std::vector<table_id_t>& tableIDs) {
    auto nodeTableIDs = getNodeTableIDs(tableIDs);
    auto queryNode = make_shared<NodeExpression>(LogicalType(LogicalTypeID::NODE),
        getUniqueExpressionName(parsedName), parsedName, nodeTableIDs);
    queryNode->setAlias(parsedName);
    bindQueryNodeProperties(*queryNode);
    queryNode->setInternalID(expressionBinder.createInternalNodeIDExpression(*queryNode));
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
    auto tableSchemas = catalog.getReadOnlyVersion()->getTableSchemas(node.getTableIDs());
    auto propertyNames = getPropertyNames(tableSchemas);
    for (auto& propertyName : propertyNames) {
        node.addPropertyExpression(
            propertyName, createPropertyExpression(propertyName, node.getUniqueName(),
                              node.getVariableName(), tableSchemas));
    }
}

std::vector<table_id_t> Binder::bindTableIDs(
    const std::vector<std::string>& tableNames, bool nodePattern) {
    auto catalogContent = catalog.getReadOnlyVersion();
    std::unordered_set<common::table_id_t> tableIDSet;
    if (tableNames.empty()) { // Rewrite empty table names as all tables.
        if (catalogContent->containsRdfGraph()) {
            // If catalog contains rdf graph then it should NOT have any property graph table.
            for (auto tableID : catalogContent->getRdfGraphIDs()) {
                tableIDSet.insert(tableID);
            }
        } else if (nodePattern) {
            if (!catalogContent->containsNodeTable()) {
                throw BinderException("No node table exists in database.");
            }
            for (auto tableID : catalogContent->getNodeTableIDs()) {
                tableIDSet.insert(tableID);
            }
        } else { // rel
            if (!catalogContent->containsRelTable()) {
                throw BinderException("No rel table exists in database.");
            }
            for (auto tableID : catalogContent->getRelTableIDs()) {
                tableIDSet.insert(tableID);
            }
        }
    } else {
        for (auto& tableName : tableNames) {
            tableIDSet.insert(bindTableID(tableName));
        }
    }
    auto result = std::vector<table_id_t>{tableIDSet.begin(), tableIDSet.end()};
    std::sort(result.begin(), result.end());
    return result;
}

std::vector<table_id_t> Binder::getNodeTableIDs(const std::vector<table_id_t>& tableIDs) {
    auto readVersion = catalog.getReadOnlyVersion();
    auto tableType = readVersion->getTableSchema(tableIDs[0])->getTableType();
    switch (tableType) {
    case TableType::RDF: { // extract node table ID from rdf graph schema.
        std::vector<table_id_t> result;
        for (auto& tableID : tableIDs) {
            auto rdfGraphSchema =
                reinterpret_cast<RdfGraphSchema*>(readVersion->getTableSchema(tableID));
            result.push_back(rdfGraphSchema->getNodeTableID());
        }
        return result;
    }
    case TableType::NODE: {
        return tableIDs;
    }
    default:
        throw NotImplementedException("Binder::getNodeTableIDs");
    }
}

std::vector<table_id_t> Binder::getRelTableIDs(const std::vector<table_id_t>& tableIDs) {
    auto readVersion = catalog.getReadOnlyVersion();
    auto tableType = readVersion->getTableSchema(tableIDs[0])->getTableType();
    switch (tableType) {
    case TableType::RDF: { // extract rel table ID from rdf graph schema.
        std::vector<table_id_t> result;
        for (auto& tableID : tableIDs) {
            auto rdfGraphSchema =
                reinterpret_cast<RdfGraphSchema*>(readVersion->getTableSchema(tableID));
            result.push_back(rdfGraphSchema->getRelTableID());
        }
        return result;
    }
    case TableType::REL_GROUP: { // extract rel table ID from rel group schema.
        std::vector<table_id_t> result;
        for (auto& tableID : tableIDs) {
            auto relGroupSchema =
                reinterpret_cast<RelTableGroupSchema*>(readVersion->getTableSchema(tableID));
            for (auto& relTableID : relGroupSchema->getRelTableIDs()) {
                result.push_back(relTableID);
            }
        }
        return result;
    }
    case TableType::REL: {
        return tableIDs;
    }
    default:
        throw NotImplementedException("Binder::getRelTableIDs");
    }
}

} // namespace binder
} // namespace kuzu
