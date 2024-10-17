#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
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
            auto node = ku_dynamic_cast<NodeExpression*>(child.get());
            extraFieldFromStructType(node->getDataType(), nodeFieldNameSet, nodeFieldNames,
                nodeFieldTypes);
        } else if (ExpressionUtil::isRelPattern(*child)) {
            auto rel = ku_dynamic_cast<RelExpression*>(child.get());
            extraFieldFromStructType(rel->getDataType(), relFieldNameSet, relFieldNames,
                relFieldTypes);
        } else if (ExpressionUtil::isRecursiveRelPattern(*child)) {
            auto recursiveRel = ku_dynamic_cast<RelExpression*>(child.get());
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
        for (auto& property : entry->getProperties()) {
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
        bool exists = false;
        if (entry->containsProperty(propertyName)) {
            exists = true;
            dataTypes.push_back(entry->getProperty(propertyName).getType().copy());
        }
        // Bind isPrimaryKey
        auto isPrimaryKey = false;
        if (entry->getTableType() == common::TableType::NODE) {
            auto nodeEntry = entry->constPtrCast<NodeTableCatalogEntry>();
            isPrimaryKey = nodeEntry->getPrimaryKeyName() == propertyName;
        }
        auto info = SingleLabelPropertyInfo(exists, isPrimaryKey);
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
    auto entries = bindTableEntries(relPattern.getTableNames(), false);
    // bind src & dst node
    RelDirectionType directionType = RelDirectionType::UNKNOWN;
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
        queryRel = createRecursiveQueryRel(relPattern, entries, srcNode, dstNode, directionType);
    } else {
        queryRel = createNonRecursiveQueryRel(relPattern.getVariableName(), entries, srcNode,
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
    const std::vector<catalog::TableCatalogEntry*>& entries,
    std::shared_ptr<NodeExpression> srcNode, std::shared_ptr<NodeExpression> dstNode,
    RelDirectionType directionType) {
    auto relTableEntries = getRelTableEntries(entries);
    auto queryRel = make_shared<RelExpression>(LogicalType(LogicalTypeID::REL),
        getUniqueExpressionName(parsedName), parsedName, relTableEntries, std::move(srcNode),
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
    auto transaction = clientContext->getTx();
    for (auto& entry : relTableEntries) {
        bool isRdfRelTable = false;
        for (auto& rdfGraphEntry : catalog->getRdfGraphEntries(transaction)) {
            if (rdfGraphEntry->isParent(entry->getTableID())) {
                KU_ASSERT(rdfGraphEntry->getTableType() == TableType::RDF);
                rdfGraphTableIDSet.insert(rdfGraphEntry->getTableID());
                isRdfRelTable = true;
            }
        }
        if (!isRdfRelTable) {
            nonRdfRelTableIDSet.insert(entry->getTableID());
        }
    }
    if (!rdfGraphTableIDSet.empty()) {
        if (!nonRdfRelTableIDSet.empty()) {
            auto relTableName =
                catalog->getTableCatalogEntry(transaction, *nonRdfRelTableIDSet.begin())->getName();
            auto rdfGraphName =
                catalog->getTableCatalogEntry(transaction, *rdfGraphTableIDSet.begin())->getName();
            throw BinderException(stringFormat(
                "Relationship pattern {} contains both PropertyGraph relationship "
                "label {} and RDFGraph label {}. Mixing relationships tables from an RDFGraph and "
                "PropertyGraph in one pattern is currently not supported.",
                parsedName, relTableName, rdfGraphName));
        }
        common::table_id_vector_t resourceTableIDs;
        for (auto& tableID : rdfGraphTableIDSet) {
            auto entry = catalog->getTableCatalogEntry(transaction, tableID);
            auto& rdfGraphEntry = entry->constCast<RDFGraphCatalogEntry>();
            resourceTableIDs.push_back(rdfGraphEntry.getResourceTableID());
        }
        auto pID =
            expressionBinder.bindNodeOrRelPropertyExpression(*queryRel, std::string(rdf::PID));
        auto rdfInfo = std::make_unique<RdfPredicateInfo>(resourceTableIDs, std::move(pID));
        queryRel->setRdfPredicateInfo(std::move(rdfInfo));
        std::vector<TableCatalogEntry*> resourceTableSchemas;
        for (auto tableID : resourceTableIDs) {
            resourceTableSchemas.push_back(catalog->getTableCatalogEntry(transaction, tableID));
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
        auto& property = expression->constCast<PropertyExpression>();
        fields.emplace_back(property.getPropertyName(), property.getDataType().copy());
    }
    auto extraInfo = std::make_unique<StructTypeInfo>(std::move(fields));
    queryRel->setExtraTypeInfo(std::move(extraInfo));
    return queryRel;
}

static void bindProjectionListAsStructField(const expression_vector& projectionList,
    std::vector<StructField>& fields) {
    for (auto& expression : projectionList) {
        if (expression->expressionType != ExpressionType::PROPERTY) {
            throw BinderException(stringFormat("Unsupported projection item {} on recursive rel.",
                expression->toString()));
        }
        auto& property = expression->constCast<PropertyExpression>();
        fields.emplace_back(property.getPropertyName(), property.getDataType().copy());
    }
}

std::shared_ptr<RelExpression> Binder::createRecursiveQueryRel(const parser::RelPattern& relPattern,
    const std::vector<catalog::TableCatalogEntry*>& entries,
    std::shared_ptr<NodeExpression> srcNode, std::shared_ptr<NodeExpression> dstNode,
    RelDirectionType directionType) {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    auto relTableEntries = getRelTableEntries(entries);
    table_catalog_entry_set_t entrySet;
    for (auto entry : relTableEntries) {
        auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
        entrySet.insert(catalog->getTableCatalogEntry(transaction, relTableEntry.getSrcTableID()));
        entrySet.insert(catalog->getTableCatalogEntry(transaction, relTableEntry.getDstTableID()));
    }
    auto recursivePatternInfo = relPattern.getRecursiveInfo();
    auto prevScope = saveScope();
    scope.clear();
    // Bind intermediate node.
    auto node = createQueryNode(recursivePatternInfo->nodeName,
        std::vector<TableCatalogEntry*>{entrySet.begin(), entrySet.end()});
    addToScope(node->toString(), node);
    std::vector<StructField> nodeFields;
    nodeFields.emplace_back(InternalKeyword::ID, node->getInternalID()->getDataType().copy());
    nodeFields.emplace_back(InternalKeyword::LABEL,
        node->getLabelExpression()->getDataType().copy());
    auto nodeProjectionList = bindRecursivePatternNodeProjectionList(*recursivePatternInfo, *node);
    bindProjectionListAsStructField(nodeProjectionList, nodeFields);
    auto nodeExtraInfo = std::make_unique<StructTypeInfo>(std::move(nodeFields));
    node->setExtraTypeInfo(std::move(nodeExtraInfo));
    auto nodeCopy = createQueryNode(recursivePatternInfo->nodeName,
        std::vector<TableCatalogEntry*>{entrySet.begin(), entrySet.end()});
    // Bind intermediate rel
    auto rel = createNonRecursiveQueryRel(recursivePatternInfo->relName, relTableEntries,
        nullptr /* srcNode */, nullptr /* dstNode */, directionType);
    addToScope(rel->toString(), rel);
    auto relProjectionList = bindRecursivePatternRelProjectionList(*recursivePatternInfo, *rel);
    std::vector<StructField> relFields;
    relFields.emplace_back(InternalKeyword::SRC, LogicalType::INTERNAL_ID());
    relFields.emplace_back(InternalKeyword::DST, LogicalType::INTERNAL_ID());
    relFields.emplace_back(InternalKeyword::LABEL, rel->getLabelExpression()->getDataType().copy());
    relFields.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    bindProjectionListAsStructField(relProjectionList, relFields);
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
    bool emptyRecursivePattern = false;
    if (recursivePatternInfo->whereExpression != nullptr) {
        auto wherePredicate = bindWhereExpression(*recursivePatternInfo->whereExpression);
        for (auto& predicate : wherePredicate->splitOnAND()) {
            auto collector = DependentVarNameCollector();
            collector.visit(predicate);
            auto dependentVariableNames = collector.getVarNames();
            auto dependOnNode = dependentVariableNames.contains(node->getUniqueName());
            auto dependOnRel = dependentVariableNames.contains(rel->getUniqueName());
            auto canNotEvaluatePredicateMessage =
                stringFormat("Cannot evaluate {} in recursive pattern {}.", predicate->toString(),
                    relPattern.getVariableName());
            if (dependOnNode && dependOnRel) {
                throw BinderException(canNotEvaluatePredicateMessage);
            } else if (dependOnNode) {
                nodePredicate = expressionBinder.combineBooleanExpressions(ExpressionType::AND,
                    nodePredicate, predicate);
            } else if (dependOnRel) {
                relPredicate = expressionBinder.combineBooleanExpressions(ExpressionType::AND,
                    relPredicate, predicate);
            } else {
                if (predicate->expressionType != common::ExpressionType::LITERAL ||
                    predicate->dataType != LogicalType::BOOL()) {
                    throw BinderException(canNotEvaluatePredicateMessage);
                }
                // If predicate is true literal, we ignore.
                // If predicate is false literal, we mark this recursive relationship as empty
                // and later in planner we replace it with EmptyResult.
                if (!predicate->constCast<LiteralExpression>().getValue().getValue<bool>()) {
                    emptyRecursivePattern = true;
                }
            }
        }
    }
    auto nodePredicateExecutionFlag = expressionBinder.createVariableExpression(LogicalType::BOOL(),
        std::string(InternalKeyword::ANONYMOUS));
    // Bind rel
    restoreScope(std::move(prevScope));
    auto parsedName = relPattern.getVariableName();
    if (emptyRecursivePattern) {
        relTableEntries.clear();
    }
    auto queryRel = std::make_shared<RelExpression>(
        getRecursiveRelLogicalType(node->getDataType(), rel->getDataType()),
        getUniqueExpressionName(parsedName), parsedName, relTableEntries, std::move(srcNode),
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
    recursiveInfo->nodePredicateExecFlag = nodePredicateExecutionFlag;
    recursiveInfo->originalNodePredicate = nodePredicate;
    if (nodePredicate != nullptr) {
        recursiveInfo->nodePredicate = expressionBinder.combineBooleanExpressions(
            ExpressionType::OR, nodePredicate, nodePredicateExecutionFlag);
    }

    recursiveInfo->relPredicate = std::move(relPredicate);
    recursiveInfo->nodeProjectionList = std::move(nodeProjectionList);
    recursiveInfo->relProjectionList = std::move(relProjectionList);

    recursiveInfo->pathNodeIDsExpr = expressionBinder.createVariableExpression(
        LogicalType::LIST(LogicalType::INTERNAL_ID()), std::string("pathNodeIDs"));
    recursiveInfo->pathEdgeIDsExpr = expressionBinder.createVariableExpression(
        LogicalType::LIST(LogicalType::INTERNAL_ID()), std::string("pathEdgeIDs"));
    recursiveInfo->pathEdgeDirectionsExpr = expressionBinder.createVariableExpression(
        LogicalType::LIST(LogicalType::BOOL()), std::string("pathEdgeDirections"));

    queryRel->setRecursiveInfo(std::move(recursiveInfo));
    return queryRel;
}

expression_vector Binder::bindRecursivePatternNodeProjectionList(
    const RecursiveRelPatternInfo& info, const NodeOrRelExpression& expr) {
    expression_vector result;
    if (!info.hasProjection) {
        for (auto& expression : expr.getPropertyExprsRef()) {
            result.push_back(expression->copy());
        }
    } else {
        for (auto& expression : info.nodeProjectionList) {
            result.push_back(expressionBinder.bindExpression(*expression));
        }
    }
    return result;
}

expression_vector Binder::bindRecursivePatternRelProjectionList(const RecursiveRelPatternInfo& info,
    const NodeOrRelExpression& expr) {
    expression_vector result;
    if (!info.hasProjection) {
        for (auto& expression : expr.getPropertyExprsRef()) {
            if (expression->constCast<PropertyExpression>().isInternalID()) {
                continue;
            }
            result.push_back(expression->copy());
        }
    } else {
        for (auto& expression : info.relProjectionList) {
            result.push_back(expressionBinder.bindExpression(*expression));
        }
    }
    return result;
}

std::pair<uint64_t, uint64_t> Binder::bindVariableLengthRelBound(
    const kuzu::parser::RelPattern& relPattern) {
    auto recursiveInfo = relPattern.getRecursiveInfo();
    uint32_t lowerBound = 0;
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
    auto entries = rel.getEntries();
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
                auto otherEntries = bindTableEntries(nodePattern.getTableNames(), true);
                auto otherNodeEntries = getNodeTableEntries(otherEntries);
                queryNode->addEntries(otherNodeEntries);
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
    return createQueryNode(parsedName, bindTableEntries(nodePattern.getTableNames(), true));
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(const std::string& parsedName,
    const std::vector<catalog::TableCatalogEntry*>& entries) {
    auto nodeEntries = getNodeTableEntries(entries);
    auto queryNode = make_shared<NodeExpression>(LogicalType(LogicalTypeID::NODE),
        getUniqueExpressionName(parsedName), parsedName, nodeEntries);
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
        auto property = ku_dynamic_cast<PropertyExpression*>(expression.get());
        fieldNames.emplace_back(property->getPropertyName());
        fieldTypes.emplace_back(property->dataType.copy());
    }
    auto extraInfo = std::make_unique<StructTypeInfo>(fieldNames, fieldTypes);
    queryNode->setExtraTypeInfo(std::move(extraInfo));
    return queryNode;
}

void Binder::bindQueryNodeProperties(NodeExpression& node) {
    auto entries = node.getEntries();
    auto propertyNames = getPropertyNames(entries);
    for (auto& propertyName : propertyNames) {
        auto property = createPropertyExpression(propertyName, node, entries);
        node.addPropertyExpression(propertyName, std::move(property));
    }
}

std::vector<TableCatalogEntry*> Binder::bindTableEntries(const std::vector<std::string>& tableNames,
    bool nodePattern) const {
    auto tx = clientContext->getTx();
    auto catalog = clientContext->getCatalog();
    table_catalog_entry_set_t entrySet;
    if (tableNames.empty()) { // Rewrite empty table names as all tables.
        if (nodePattern) {    // Fill all node table schemas to node pattern.
            for (auto entry : catalog->getNodeTableEntries(tx)) {
                entrySet.insert(entry);
            }
        } else { // Fill all rel table schemas to rel pattern.
            for (auto entry : catalog->getRelTableEntries(tx)) {
                entrySet.insert(entry);
            }
        }
    } else {
        for (auto& tableName : tableNames) {
            entrySet.insert(bindTableEntry(tableName));
        }
    }
    std::vector<TableCatalogEntry*> entries;
    for (auto entry : entrySet) {
        entries.push_back(entry);
    }
    std::sort(entries.begin(), entries.end(), [](TableCatalogEntry* a, TableCatalogEntry* b) {
        return a->getTableID() < b->getTableID();
    });
    return entries;
}

catalog::TableCatalogEntry* Binder::bindTableEntry(const std::string& tableName) const {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    if (!catalog->containsTable(transaction, tableName)) {
        throw BinderException(common::stringFormat("Table {} does not exist.", tableName));
    }
    return catalog->getTableCatalogEntry(transaction, tableName);
}

common::table_id_t Binder::bindTableID(const std::string& tableName) const {
    return bindTableEntry(tableName)->getTableID();
}

std::vector<TableCatalogEntry*> Binder::getNodeTableEntries(
    const std::vector<TableCatalogEntry*>& entries) const {
    return getTableEntries(entries, TableType::NODE);
}

std::vector<TableCatalogEntry*> Binder::getRelTableEntries(
    const std::vector<TableCatalogEntry*>& entries) const {
    return getTableEntries(entries, TableType::REL);
}

std::vector<TableCatalogEntry*> Binder::getTableEntries(
    const std::vector<TableCatalogEntry*>& entries, TableType tableType) const {
    std::vector<TableCatalogEntry*> result;
    table_id_set_t set;
    for (auto& entry : entries) {
        std::vector<TableCatalogEntry*> expandedEntries;
        switch (tableType) {
        case TableType::NODE: {
            expandedEntries = getNodeTableEntries(entry);
        } break;
        case TableType::REL: {
            expandedEntries = getRelTableEntries(entry);
        } break;
        default:
            break;
        }
        for (auto& e : expandedEntries) {
            if (set.contains(e->getTableID())) {
                continue;
            }
            result.push_back(e);
            set.insert(e->getTableID());
        }
    }
    return result;
}

std::vector<TableCatalogEntry*> Binder::getNodeTableEntries(TableCatalogEntry* entry) const {
    switch (entry->getTableType()) {
    case TableType::NODE: {
        return {entry};
    }
    default:
        throw BinderException(
            stringFormat("Cannot bind {} as a node pattern label.", entry->getName()));
    }
}

std::vector<TableCatalogEntry*> Binder::getRelTableEntries(TableCatalogEntry* entry) const {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    switch (entry->getTableType()) {
    case TableType::RDF: {
        auto& rdfEntry = entry->constCast<RDFGraphCatalogEntry>();
        auto rtEntry =
            catalog->getTableCatalogEntry(transaction, rdfEntry.getResourceTripleTableID());
        auto ltEntry =
            catalog->getTableCatalogEntry(transaction, rdfEntry.getLiteralTripleTableID());
        return {rtEntry, ltEntry};
    }
    case TableType::REL_GROUP: {
        auto& relGroupEntry = entry->constCast<RelGroupCatalogEntry>();
        std::vector<TableCatalogEntry*> result;
        for (auto id : relGroupEntry.getRelTableIDs()) {
            result.push_back(catalog->getTableCatalogEntry(transaction, id));
        }
        return result;
    }
    case TableType::REL: {
        return {entry};
    }
    default:
        throw BinderException(
            stringFormat("Cannot bind {} as a relationship pattern label.", entry->getName()));
    }
}

std::vector<TableCatalogEntry*> Binder::getTableEntries(const table_id_vector_t& tableIDs) {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    std::vector<TableCatalogEntry*> result;
    for (auto id : tableIDs) {
        result.push_back(catalog->getTableCatalogEntry(transaction, id));
    }
    return result;
}

} // namespace binder
} // namespace kuzu
