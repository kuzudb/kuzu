#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/path_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_visitor.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/keyword/rdf_keyword.h"
#include "common/string_format.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "main/client_context.h"

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
BoundGraphPattern Binder::bindGraphPattern(const std::vector<PatternElement>& graphPattern) {
    auto queryGraphCollection = QueryGraphCollection();
    for (auto& patternElement : graphPattern) {
        queryGraphCollection.addAndMergeQueryGraphIfConnected(bindPatternElement(patternElement));
    }
    queryGraphCollection.finalize();
    auto boundPattern = BoundGraphPattern();
    boundPattern.queryGraphCollection = std::move(queryGraphCollection);
    return boundPattern;
}

// Grammar ensures pattern element is always connected and thus can be bound as a query graph.
QueryGraph Binder::bindPatternElement(const PatternElement& patternElement) {
    auto queryGraph = QueryGraph();
    expression_vector nodeAndRels;
    auto leftNode = bindQueryNode(*patternElement.getFirstNodePattern(), queryGraph);
    nodeAndRels.push_back(leftNode);
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto patternElementChain = patternElement.getPatternElementChain(i);
        auto rightNode = bindQueryNode(*patternElementChain->getNodePattern(), queryGraph);
        auto rel =
            bindQueryRel(*patternElementChain->getRelPattern(), leftNode, rightNode, queryGraph);
        nodeAndRels.push_back(rel);
        nodeAndRels.push_back(rightNode);
        leftNode = rightNode;
    }
    if (patternElement.hasPathName()) {
        auto pathName = patternElement.getPathName();
        auto pathExpression = createPath(pathName, nodeAndRels);
        addToScope(pathName, pathExpression);
    }
    return queryGraph;
}

static LogicalType getRecursiveRelLogicalType(const LogicalType& nodeType,
    const LogicalType& relType) {
    auto nodesType = LogicalType::LIST(nodeType.copy());
    auto relsType = LogicalType::LIST(relType.copy());
    std::vector<StructField> recursiveRelFields;
    recursiveRelFields.emplace_back(InternalKeyword::NODES, std::move(nodesType));
    recursiveRelFields.emplace_back(InternalKeyword::RELS, std::move(relsType));
    return LogicalType::RECURSIVE_REL(
        std::make_unique<StructTypeInfo>(std::move(recursiveRelFields)));
}

static void extraFieldFromStructType(const LogicalType& structType,
    std::unordered_set<std::string>& nameSet, std::vector<std::string>& names,
    std::vector<LogicalType>& types) {
    for (auto& field : StructType::getFields(structType)) {
        if (!nameSet.contains(field.getName())) {
            nameSet.insert(field.getName());
            names.push_back(field.getName());
            types.push_back(field.getType().copy());
        }
    }
}

std::shared_ptr<Expression> Binder::createPath(const std::string& pathName,
    const expression_vector& children) {
    std::unordered_set<std::string> nodeFieldNameSet;
    std::vector<std::string> nodeFieldNames;
    std::vector<LogicalType> nodeFieldTypes;
    std::unordered_set<std::string> relFieldNameSet;
    std::vector<std::string> relFieldNames;
    std::vector<LogicalType> relFieldTypes;
    for (auto& child : children) {
        if (ExpressionUtil::isNodePattern(*child)) {
            auto node = ku_dynamic_cast<Expression*, NodeExpression*>(child.get());
            extraFieldFromStructType(node->getDataType(), nodeFieldNameSet, nodeFieldNames,
                nodeFieldTypes);
        } else if (ExpressionUtil::isRelPattern(*child)) {
            auto rel = ku_dynamic_cast<Expression*, RelExpression*>(child.get());
            extraFieldFromStructType(rel->getDataType(), relFieldNameSet, relFieldNames,
                relFieldTypes);
        } else if (ExpressionUtil::isRecursiveRelPattern(*child)) {
            auto recursiveRel = ku_dynamic_cast<Expression*, RelExpression*>(child.get());
            auto recursiveInfo = recursiveRel->getRecursiveInfo();
            extraFieldFromStructType(recursiveInfo->node->getDataType(), nodeFieldNameSet,
                nodeFieldNames, nodeFieldTypes);
            extraFieldFromStructType(recursiveInfo->rel->getDataType(), relFieldNameSet,
                relFieldNames, relFieldTypes);
        } else {
            KU_UNREACHABLE;
        }
    }
    auto nodeExtraInfo = std::make_unique<StructTypeInfo>(nodeFieldNames, nodeFieldTypes);
    auto nodeType = LogicalType::NODE(std::move(nodeExtraInfo));
    auto relExtraInfo = std::make_unique<StructTypeInfo>(relFieldNames, relFieldTypes);
    auto relType = LogicalType::REL(std::move(relExtraInfo));
    auto uniqueName = getUniqueExpressionName(pathName);
    return std::make_shared<PathExpression>(getRecursiveRelLogicalType(nodeType, relType),
        uniqueName, pathName, std::move(nodeType), std::move(relType), children);
}

static std::vector<std::string> getPropertyNames(const std::vector<TableCatalogEntry*>& entries) {
    std::vector<std::string> result;
    std::unordered_set<std::string> propertyNamesSet;
    for (auto& entry : entries) {
        for (auto& property : entry->getPropertiesRef()) {
            if (propertyNamesSet.contains(property.getName())) {
                continue;
            }
            propertyNamesSet.insert(property.getName());
            result.push_back(property.getName());
        }
    }
    return result;
}

static std::unique_ptr<Expression> createPropertyExpression(const std::string& propertyName,
    const std::string& uniqueVariableName, const std::string& rawVariableName,
    const std::vector<TableCatalogEntry*>& entries) {
    common::table_id_map_t<SingleLabelPropertyInfo> infos;
    std::vector<LogicalType> dataTypes;
    for (auto& entry : entries) {
        // Bind property id
        auto propertyID = INVALID_PROPERTY_ID;
        if (entry->containProperty(propertyName)) {
            propertyID = entry->getPropertyID(propertyName);
            dataTypes.push_back(entry->getProperty(propertyID)->getDataType().copy());
        }
        // Bind isPrimaryKey
        auto isPrimaryKey = false;
        if (entry->getTableType() == common::TableType::NODE) {
            auto nodeEntry = entry->constPtrCast<NodeTableCatalogEntry>();
            isPrimaryKey = nodeEntry->getPrimaryKeyPID() == propertyID;
        }
        auto info = SingleLabelPropertyInfo(isPrimaryKey, propertyID);
        infos.insert({entry->getTableID(), std::move(info)});
    }
    // Validate property under the same name has the same type.
    KU_ASSERT(!dataTypes.empty());
    for (const auto& type : dataTypes) {
        if (dataTypes[0] != type) {
            throw BinderException(
                stringFormat("Expected the same data type for property {} but found {} and {}.",
                    propertyName, type.toString(), dataTypes[0].toString()));
        }
    }
    return make_unique<PropertyExpression>(std::move(dataTypes[0]), propertyName,
        uniqueVariableName, rawVariableName, std::move(infos));
}

static std::unique_ptr<Expression> createPropertyExpression(const std::string& propertyName,
    const Expression& pattern, const std::vector<TableCatalogEntry*>& entries) {
    return createPropertyExpression(propertyName, pattern.getUniqueName(), pattern.toString(),
        entries);
}

std::shared_ptr<RelExpression> Binder::bindQueryRel(const RelPattern& relPattern,
    const std::shared_ptr<NodeExpression>& leftNode,
    const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph) {
    auto parsedName = relPattern.getVariableName();
    if (scope.contains(parsedName)) {
        auto prevVariable = scope.getExpression(parsedName);
        auto expectedDataType = QueryRelTypeUtils::isRecursive(relPattern.getRelType()) ?
                                    LogicalTypeID::RECURSIVE_REL :
                                    LogicalTypeID::REL;
        ExpressionUtil::validateDataType(*prevVariable, expectedDataType);
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
        KU_UNREACHABLE;
    }
    // bind variable length
    std::shared_ptr<RelExpression> queryRel;
    if (QueryRelTypeUtils::isRecursive(relPattern.getRelType())) {
        queryRel = createRecursiveQueryRel(relPattern, tableIDs, srcNode, dstNode, directionType);
    } else {
        queryRel = createNonRecursiveQueryRel(relPattern.getVariableName(), tableIDs, srcNode,
            dstNode, directionType);
        for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
            auto boundLhs =
                expressionBinder.bindNodeOrRelPropertyExpression(*queryRel, propertyName);
            auto boundRhs = expressionBinder.bindExpression(*rhs);
            boundRhs = expressionBinder.implicitCastIfNecessary(boundRhs, boundLhs->dataType);
            queryRel->addPropertyDataExpr(propertyName, std::move(boundRhs));
        }
    }
    queryRel->setAlias(parsedName);
    if (!parsedName.empty()) {
        addToScope(parsedName, queryRel);
    }
    queryGraph.addQueryRel(queryRel);
    return queryRel;
}

std::shared_ptr<RelExpression> Binder::createNonRecursiveQueryRel(const std::string& parsedName,
    const std::vector<table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    auto relTableIDs = getRelTableIDs(tableIDs);
    auto queryRel = make_shared<RelExpression>(LogicalType(LogicalTypeID::REL),
        getUniqueExpressionName(parsedName), parsedName, relTableIDs, std::move(srcNode),
        std::move(dstNode), directionType, QueryRelType::NON_RECURSIVE);
    if (directionType == RelDirectionType::BOTH) {
        queryRel->setDirectionExpr(expressionBinder.createVariableExpression(LogicalType::BOOL(),
            queryRel->getUniqueName() + InternalKeyword::DIRECTION));
    }
    queryRel->setAlias(parsedName);
    bindQueryRelProperties(*queryRel);
    // Try bind rdf rel table.
    // For rdf rel table, we store predicate ID instead of predicate IRI. However, we need to hide
    // this information from the user. The following code block tries to create a IRI property
    // expression to mock as if predicate IRI exists in rel table.
    common::table_id_set_t rdfGraphTableIDSet;
    common::table_id_set_t nonRdfRelTableIDSet;
    auto catalog = clientContext->getCatalog();
    for (auto& tableID : relTableIDs) {
        bool isRdfRelTable = false;
        for (auto& rdfGraphEntry : catalog->getRdfGraphEntries(clientContext->getTx())) {
            if (rdfGraphEntry->isParent(tableID)) {
                KU_ASSERT(rdfGraphEntry->getTableType() == TableType::RDF);
                rdfGraphTableIDSet.insert(rdfGraphEntry->getTableID());
                isRdfRelTable = true;
            }
        }
        if (!isRdfRelTable) {
            nonRdfRelTableIDSet.insert(tableID);
        }
    }
    if (!rdfGraphTableIDSet.empty()) {
        if (!nonRdfRelTableIDSet.empty()) {
            auto relTableName =
                catalog->getTableCatalogEntry(clientContext->getTx(), *nonRdfRelTableIDSet.begin())
                    ->getName();
            auto rdfGraphName =
                catalog->getTableCatalogEntry(clientContext->getTx(), *rdfGraphTableIDSet.begin())
                    ->getName();
            throw BinderException(stringFormat(
                "Relationship pattern {} contains both PropertyGraph relationship "
                "label {} and RDFGraph label {}. Mixing relationships tables from an RDFGraph and "
                "PropertyGraph in one pattern is currently not supported.",
                parsedName, relTableName, rdfGraphName));
        }
        common::table_id_vector_t resourceTableIDs;
        for (auto& tableID : rdfGraphTableIDSet) {
            auto entry = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
            auto rdfGraphEntry =
                ku_dynamic_cast<const TableCatalogEntry*, const RDFGraphCatalogEntry*>(entry);
            resourceTableIDs.push_back(rdfGraphEntry->getResourceTableID());
        }
        auto pID =
            expressionBinder.bindNodeOrRelPropertyExpression(*queryRel, std::string(rdf::PID));
        auto rdfInfo = std::make_unique<RdfPredicateInfo>(resourceTableIDs, std::move(pID));
        queryRel->setRdfPredicateInfo(std::move(rdfInfo));
        std::vector<TableCatalogEntry*> resourceTableSchemas;
        for (auto tableID : resourceTableIDs) {
            resourceTableSchemas.push_back(
                catalog->getTableCatalogEntry(clientContext->getTx(), tableID));
        }
        // Mock existence of pIRI property.
        auto pIRI =
            createPropertyExpression(std::string(rdf::IRI), *queryRel, resourceTableSchemas);
        queryRel->addPropertyExpression(std::string(rdf::IRI), std::move(pIRI));
    }
    std::vector<StructField> fields;
    fields.emplace_back(InternalKeyword::SRC, LogicalType::INTERNAL_ID());
    fields.emplace_back(InternalKeyword::DST, LogicalType::INTERNAL_ID());
    // Bind internal expressions.
    queryRel->setLabelExpression(expressionBinder.bindLabelFunction(*queryRel));
    fields.emplace_back(InternalKeyword::LABEL,
        queryRel->getLabelExpression()->getDataType().copy());
    // Bind properties.
    for (auto& expression : queryRel->getPropertyExprsRef()) {
        auto property = ku_dynamic_cast<Expression*, PropertyExpression*>(expression.get());
        fields.emplace_back(property->getPropertyName(), property->getDataType().copy());
    }
    auto extraInfo = std::make_unique<StructTypeInfo>(std::move(fields));
    queryRel->setExtraTypeInfo(std::move(extraInfo));
    return queryRel;
}

static void bindRecursiveRelProjectionList(const expression_vector& projectionList,
    std::vector<StructField>& fields) {
    for (auto& expression : projectionList) {
        if (expression->expressionType != common::ExpressionType::PROPERTY) {
            throw BinderException(stringFormat("Unsupported projection item {} on recursive rel.",
                expression->toString()));
        }
        auto property = ku_dynamic_cast<Expression*, PropertyExpression*>(expression.get());
        fields.emplace_back(property->getPropertyName(), property->getDataType().copy());
    }
}

std::shared_ptr<RelExpression> Binder::createRecursiveQueryRel(const parser::RelPattern& relPattern,
    const std::vector<table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    auto relTableIDs = getRelTableIDs(tableIDs);
    std::unordered_set<table_id_t> nodeTableIDs;
    for (auto relTableID : relTableIDs) {
        auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), relTableID));
        nodeTableIDs.insert(relTableEntry->getSrcTableID());
        nodeTableIDs.insert(relTableEntry->getDstTableID());
    }
    auto recursivePatternInfo = relPattern.getRecursiveInfo();
    auto prevScope = saveScope();
    scope.clear();
    // Bind intermediate node.
    auto node = createQueryNode(recursivePatternInfo->nodeName,
        std::vector<table_id_t>{nodeTableIDs.begin(), nodeTableIDs.end()});
    addToScope(node->toString(), node);
    std::vector<StructField> nodeFields;
    nodeFields.emplace_back(InternalKeyword::ID, node->getInternalID()->getDataType().copy());
    nodeFields.emplace_back(InternalKeyword::LABEL,
        node->getLabelExpression()->getDataType().copy());
    expression_vector nodeProjectionList;
    if (!recursivePatternInfo->hasProjection) {
        for (auto& expression : node->getPropertyExprsRef()) {
            nodeProjectionList.push_back(expression->copy());
        }
    } else {
        for (auto& expression : recursivePatternInfo->nodeProjectionList) {
            nodeProjectionList.push_back(expressionBinder.bindExpression(*expression));
        }
    }
    bindRecursiveRelProjectionList(nodeProjectionList, nodeFields);
    auto nodeExtraInfo = std::make_unique<StructTypeInfo>(std::move(nodeFields));
    node->setExtraTypeInfo(std::move(nodeExtraInfo));
    auto nodeCopy = createQueryNode(recursivePatternInfo->nodeName,
        std::vector<table_id_t>{nodeTableIDs.begin(), nodeTableIDs.end()});
    // Bind intermediate rel
    auto rel = createNonRecursiveQueryRel(recursivePatternInfo->relName, tableIDs, node, nodeCopy,
        directionType);
    addToScope(rel->toString(), rel);
    expression_vector relProjectionList;
    if (!recursivePatternInfo->hasProjection) {
        for (auto& expression : rel->getPropertyExprsRef()) {
            if (((PropertyExpression*)expression.get())->isInternalID()) {
                continue;
            }
            relProjectionList.push_back(expression->copy());
        }
    } else {
        for (auto& expression : recursivePatternInfo->relProjectionList) {
            relProjectionList.push_back(expressionBinder.bindExpression(*expression));
        }
    }
    std::vector<StructField> relFields;
    relFields.emplace_back(InternalKeyword::SRC, LogicalType::INTERNAL_ID());
    relFields.emplace_back(InternalKeyword::DST, LogicalType::INTERNAL_ID());
    relFields.emplace_back(InternalKeyword::LABEL, rel->getLabelExpression()->getDataType().copy());
    relFields.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    bindRecursiveRelProjectionList(relProjectionList, relFields);
    auto relExtraInfo = std::make_unique<StructTypeInfo>(std::move(relFields));
    rel->setExtraTypeInfo(std::move(relExtraInfo));
    // Bind predicates in {}, e.g. [e* {date=1999-01-01}]
    std::shared_ptr<Expression> relPredicate;
    for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
        auto boundLhs = expressionBinder.bindNodeOrRelPropertyExpression(*rel, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = expressionBinder.implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        auto predicate = expressionBinder.createEqualityComparisonExpression(boundLhs, boundRhs);
        relPredicate = expressionBinder.combineBooleanExpressions(ExpressionType::AND, relPredicate,
            predicate);
    }
    // Bind predicates in (r, n | WHERE )
    std::shared_ptr<Expression> nodePredicate;
    if (recursivePatternInfo->whereExpression != nullptr) {
        auto recursivePatternPredicate =
            expressionBinder.bindExpression(*recursivePatternInfo->whereExpression);
        for (auto& predicate : recursivePatternPredicate->splitOnAND()) {
            auto collector = DependentVarNameCollector();
            collector.visit(predicate);
            auto dependentVariableNames = collector.getVarNames();
            auto dependOnNode = dependentVariableNames.contains(node->getUniqueName());
            auto dependOnRel = dependentVariableNames.contains(rel->getUniqueName());
            if (dependOnNode && dependOnRel) {
                throw BinderException(stringFormat("Cannot evaluate {} in recursive pattern {}.",
                    predicate->toString(), relPattern.getVariableName()));
            } else if (dependOnNode) {
                nodePredicate = expressionBinder.combineBooleanExpressions(ExpressionType::AND,
                    nodePredicate, predicate);
            } else {
                relPredicate = expressionBinder.combineBooleanExpressions(ExpressionType::AND,
                    relPredicate, predicate);
            }
        }
    }
    auto nodePredicateExecutionFlag = expressionBinder.createVariableExpression(
        LogicalType{LogicalTypeID::BOOL}, std::string(InternalKeyword::ANONYMOUS));
    if (nodePredicate != nullptr) {
        nodePredicate = expressionBinder.combineBooleanExpressions(ExpressionType::OR,
            nodePredicate, nodePredicateExecutionFlag);
    }
    // Bind rel
    restoreScope(std::move(prevScope));
    auto parsedName = relPattern.getVariableName();
    auto queryRel = make_shared<RelExpression>(
        getRecursiveRelLogicalType(node->getDataType(), rel->getDataType()),
        getUniqueExpressionName(parsedName), parsedName, relTableIDs, std::move(srcNode),
        std::move(dstNode), directionType, relPattern.getRelType());
    auto lengthExpression =
        PropertyExpression::construct(LogicalType::INT64(), InternalKeyword::LENGTH, *queryRel);
    auto [lowerBound, upperBound] = bindVariableLengthRelBound(relPattern);
    auto recursiveInfo = std::make_unique<RecursiveInfo>();
    recursiveInfo->lowerBound = lowerBound;
    recursiveInfo->upperBound = upperBound;
    recursiveInfo->node = std::move(node);
    recursiveInfo->nodeCopy = std::move(nodeCopy);
    recursiveInfo->rel = std::move(rel);
    recursiveInfo->lengthExpression = std::move(lengthExpression);
    recursiveInfo->nodePredicateExecFlag = std::move(nodePredicateExecutionFlag);
    recursiveInfo->nodePredicate = std::move(nodePredicate);
    recursiveInfo->relPredicate = std::move(relPredicate);
    recursiveInfo->nodeProjectionList = std::move(nodeProjectionList);
    recursiveInfo->relProjectionList = std::move(relProjectionList);
    queryRel->setRecursiveInfo(std::move(recursiveInfo));
    return queryRel;
}

std::pair<uint64_t, uint64_t> Binder::bindVariableLengthRelBound(
    const kuzu::parser::RelPattern& relPattern) {
    auto recursiveInfo = relPattern.getRecursiveInfo();
    uint32_t lowerBound;
    function::CastString::operation(
        ku_string_t{recursiveInfo->lowerBound.c_str(), recursiveInfo->lowerBound.length()},
        lowerBound);
    auto maxDepth = clientContext->getClientConfig()->varLengthMaxDepth;
    auto upperBound = maxDepth;
    if (!recursiveInfo->upperBound.empty()) {
        function::CastString::operation(
            ku_string_t{recursiveInfo->upperBound.c_str(), recursiveInfo->upperBound.length()},
            upperBound);
    }
    if (lowerBound > upperBound) {
        throw BinderException(stringFormat("Lower bound of rel {} is greater than upperBound.",
            relPattern.getVariableName()));
    }
    if (upperBound > maxDepth) {
        throw BinderException(stringFormat("Upper bound of rel {} exceeds maximum: {}.",
            relPattern.getVariableName(), std::to_string(maxDepth)));
    }
    if ((relPattern.getRelType() == QueryRelType::ALL_SHORTEST ||
            relPattern.getRelType() == QueryRelType::SHORTEST) &&
        lowerBound != 1) {
        throw BinderException("Lower bound of shortest/all_shortest path must be 1.");
    }
    return std::make_pair(lowerBound, upperBound);
}

void Binder::bindQueryRelProperties(RelExpression& rel) {
    if (rel.isEmpty()) {
        auto internalID =
            PropertyExpression::construct(LogicalType::INTERNAL_ID(), InternalKeyword::ID, rel);
        rel.addPropertyExpression(InternalKeyword::ID, std::move(internalID));
        return;
    }
    auto catalog = clientContext->getCatalog();
    auto entries = catalog->getTableEntries(clientContext->getTx(), rel.getTableIDs());
    auto propertyNames = getPropertyNames(entries);
    for (auto& propertyName : propertyNames) {
        auto property = createPropertyExpression(propertyName, rel, entries);
        rel.addPropertyExpression(propertyName, std::move(property));
    }
}

std::shared_ptr<NodeExpression> Binder::bindQueryNode(const NodePattern& nodePattern,
    QueryGraph& queryGraph) {
    auto parsedName = nodePattern.getVariableName();
    std::shared_ptr<NodeExpression> queryNode;
    if (scope.contains(parsedName)) { // bind to node in scope
        auto prevVariable = scope.getExpression(parsedName);
        if (!ExpressionUtil::isNodePattern(*prevVariable)) {
            if (!scope.hasNodeReplacement(parsedName)) {
                throw BinderException(stringFormat("Cannot bind {} as node pattern.", parsedName));
            }
            queryNode = scope.getNodeReplacement(parsedName);
            queryNode->addPropertyDataExpr(InternalKeyword::ID, queryNode->getInternalID());
        } else {
            queryNode = std::static_pointer_cast<NodeExpression>(prevVariable);
            // E.g. MATCH (a:person) MATCH (a:organisation)
            // We bind to a single node with both labels
            if (!nodePattern.getTableNames().empty()) {
                auto otherTableIDs = bindTableIDs(nodePattern.getTableNames(), true);
                auto otherNodeTableIDs = getNodeTableIDs(otherTableIDs);
                queryNode->addTableIDs(otherNodeTableIDs);
            }
        }
    } else {
        queryNode = createQueryNode(nodePattern);
        if (!parsedName.empty()) {
            addToScope(parsedName, queryNode);
        }
    }
    for (auto& [propertyName, rhs] : nodePattern.getPropertyKeyVals()) {
        auto boundLhs = expressionBinder.bindNodeOrRelPropertyExpression(*queryNode, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = expressionBinder.implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        queryNode->addPropertyDataExpr(propertyName, std::move(boundRhs));
    }
    queryGraph.addQueryNode(queryNode);
    return queryNode;
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(const NodePattern& nodePattern) {
    auto parsedName = nodePattern.getVariableName();
    return createQueryNode(parsedName, bindTableIDs(nodePattern.getTableNames(), true));
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(const std::string& parsedName,
    const std::vector<table_id_t>& tableIDs) {
    auto nodeTableIDs = getNodeTableIDs(tableIDs);
    auto queryNode = make_shared<NodeExpression>(LogicalType(LogicalTypeID::NODE),
        getUniqueExpressionName(parsedName), parsedName, nodeTableIDs);
    queryNode->setAlias(parsedName);
    std::vector<std::string> fieldNames;
    std::vector<LogicalType> fieldTypes;
    // Bind internal expressions
    queryNode->setInternalID(
        PropertyExpression::construct(LogicalType::INTERNAL_ID(), InternalKeyword::ID, *queryNode));
    queryNode->setLabelExpression(expressionBinder.bindLabelFunction(*queryNode));
    fieldNames.emplace_back(InternalKeyword::ID);
    fieldNames.emplace_back(InternalKeyword::LABEL);
    fieldTypes.push_back(queryNode->getInternalID()->getDataType().copy());
    fieldTypes.push_back(queryNode->getLabelExpression()->getDataType().copy());
    // Bind properties.
    bindQueryNodeProperties(*queryNode);
    for (auto& expression : queryNode->getPropertyExprsRef()) {
        auto property = ku_dynamic_cast<Expression*, PropertyExpression*>(expression.get());
        fieldNames.emplace_back(property->getPropertyName());
        fieldTypes.emplace_back(property->dataType.copy());
    }
    auto extraInfo = std::make_unique<StructTypeInfo>(fieldNames, fieldTypes);
    queryNode->setExtraTypeInfo(std::move(extraInfo));
    return queryNode;
}

void Binder::bindQueryNodeProperties(NodeExpression& node) {
    auto catalog = clientContext->getCatalog();
    auto entries = catalog->getTableEntries(clientContext->getTx(), node.getTableIDs());
    auto propertyNames = getPropertyNames(entries);
    for (auto& propertyName : propertyNames) {
        auto property = createPropertyExpression(propertyName, node, entries);
        node.addPropertyExpression(propertyName, std::move(property));
    }
}

std::vector<table_id_t> Binder::bindTableIDs(const std::vector<std::string>& tableNames,
    bool nodePattern) {
    auto tx = clientContext->getTx();
    auto catalog = clientContext->getCatalog();
    std::unordered_set<common::table_id_t> tableIDSet;
    if (tableNames.empty()) { // Rewrite empty table names as all tables.
        if (nodePattern) {    // Fill all node table schemas to node pattern.
            for (auto tableID : catalog->getNodeTableIDs(tx)) {
                tableIDSet.insert(tableID);
            }
        } else { // Fill all rel table schemas to rel pattern.
            for (auto tableID : catalog->getRelTableIDs(tx)) {
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
    common::table_id_vector_t result; // use vector to preserve input order.
    common::table_id_set_t tableIDSet;
    for (auto& tableID : tableIDs) {
        for (auto& id : getNodeTableIDs(tableID)) {
            if (tableIDSet.contains(id)) {
                continue;
            }
            result.push_back(id);
            tableIDSet.insert(id);
        }
    }
    return result;
}

std::vector<common::table_id_t> Binder::getNodeTableIDs(table_id_t tableID) {
    auto tableSchema =
        clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), tableID);
    switch (tableSchema->getTableType()) {
    case TableType::NODE: {
        return {tableID};
    }
    default:
        throw BinderException(
            stringFormat("Cannot bind {} as a node pattern label.", tableSchema->getName()));
    }
}

std::vector<table_id_t> Binder::getRelTableIDs(const std::vector<table_id_t>& tableIDs) {
    common::table_id_vector_t result; // use vector to preserve input order.
    common::table_id_set_t tableIDSet;
    for (auto& tableID : tableIDs) {
        for (auto& id : getRelTableIDs(tableID)) {
            if (tableIDSet.contains(id)) {
                continue;
            }
            result.push_back(id);
            tableIDSet.insert(id);
        }
    }
    return result;
}

std::vector<common::table_id_t> Binder::getRelTableIDs(table_id_t tableID) {
    auto entry = clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), tableID);
    switch (entry->getTableType()) {
    case TableType::RDF: {
        auto rdfEntry = ku_dynamic_cast<TableCatalogEntry*, RDFGraphCatalogEntry*>(entry);
        return {rdfEntry->getResourceTripleTableID(), rdfEntry->getLiteralTripleTableID()};
    }
    case TableType::REL_GROUP: {
        auto relGroupEntry = ku_dynamic_cast<TableCatalogEntry*, RelGroupCatalogEntry*>(entry);
        return relGroupEntry->getRelTableIDs();
    }
    case TableType::REL: {
        return {tableID};
    }
    default:
        throw BinderException(
            stringFormat("Cannot bind {} as a relationship pattern label.", entry->getName()));
    }
}

} // namespace binder
} // namespace kuzu
