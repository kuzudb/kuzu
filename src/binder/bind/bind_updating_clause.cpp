#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/query/query_graph_label_analyzer.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_insert_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/assert.h"
#include "common/exception/binder.h"
#include "common/keyword/rdf_keyword.h"
#include "common/string_format.h"
#include "function/rdf/vector_rdf_functions.h"
#include "main/client_context.h"
#include "parser/query/updating_clause/delete_clause.h"
#include "parser/query/updating_clause/insert_clause.h"
#include "parser/query/updating_clause/merge_clause.h"
#include "parser/query/updating_clause/set_clause.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundUpdatingClause> Binder::bindUpdatingClause(
    const UpdatingClause& updatingClause) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::INSERT: {
        return bindInsertClause(updatingClause);
    }
    case ClauseType::MERGE: {
        return bindMergeClause(updatingClause);
    }
    case ClauseType::SET: {
        return bindSetClause(updatingClause);
    }
    case ClauseType::DELETE_: {
        return bindDeleteClause(updatingClause);
    }
    default:
        KU_UNREACHABLE;
    }
}

static std::unordered_set<std::string> populatePatternsScope(const BinderScope& scope) {
    std::unordered_set<std::string> result;
    for (auto& expression : scope.getExpressions()) {
        if (ExpressionUtil::isNodePattern(*expression) ||
            ExpressionUtil::isRelPattern(*expression)) {
            result.insert(expression->toString());
        } else if (expression->expressionType == ExpressionType::VARIABLE) {
            if (scope.hasNodeReplacement(expression->toString())) {
                result.insert(expression->toString());
            }
        }
    }
    return result;
}

std::unique_ptr<BoundUpdatingClause> Binder::bindInsertClause(
    const UpdatingClause& updatingClause) {
    auto& insertClause = (InsertClause&)updatingClause;
    auto patternsScope = populatePatternsScope(scope);
    // bindGraphPattern will update scope.
    auto boundGraphPattern = bindGraphPattern(insertClause.getPatternElementsRef());
    auto insertInfos = bindInsertInfos(boundGraphPattern.queryGraphCollection, patternsScope);
    return std::make_unique<BoundInsertClause>(std::move(insertInfos));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindMergeClause(
    const parser::UpdatingClause& updatingClause) {
    auto& mergeClause = (MergeClause&)updatingClause;
    auto patternsScope = populatePatternsScope(scope);
    // bindGraphPattern will update scope.
    auto boundGraphPattern = bindGraphPattern(mergeClause.getPatternElementsRef());
    rewriteMatchPattern(boundGraphPattern);
    auto existenceMark = createVariable("__existence", LogicalType::BOOL());
    auto distinctMark = createVariable("__distinct", LogicalType::BOOL());
    auto createInfos = bindInsertInfos(boundGraphPattern.queryGraphCollection, patternsScope);
    auto boundMergeClause = std::make_unique<BoundMergeClause>(std::move(existenceMark),
        std::move(distinctMark), std::move(boundGraphPattern.queryGraphCollection),
        std::move(boundGraphPattern.where), std::move(createInfos));
    if (mergeClause.hasOnMatchSetItems()) {
        for (auto& [lhs, rhs] : mergeClause.getOnMatchSetItemsRef()) {
            auto setPropertyInfo = bindSetPropertyInfo(lhs.get(), rhs.get());
            boundMergeClause->addOnMatchSetPropertyInfo(std::move(setPropertyInfo));
        }
    }
    if (mergeClause.hasOnCreateSetItems()) {
        for (auto& [lhs, rhs] : mergeClause.getOnCreateSetItemsRef()) {
            auto setPropertyInfo = bindSetPropertyInfo(lhs.get(), rhs.get());
            boundMergeClause->addOnCreateSetPropertyInfo(std::move(setPropertyInfo));
        }
    }
    return boundMergeClause;
}

std::vector<BoundInsertInfo> Binder::bindInsertInfos(QueryGraphCollection& queryGraphCollection,
    const std::unordered_set<std::string>& patternsInScope_) {
    auto patternsInScope = patternsInScope_;
    std::vector<BoundInsertInfo> result;
    auto analyzer = QueryGraphLabelAnalyzer(*clientContext, true /* throwOnViolate */);
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection.getQueryGraphUnsafe(i);
        // Ensure query graph does not violate declared schema.
        analyzer.pruneLabel(*queryGraph);
        for (auto j = 0u; j < queryGraph->getNumQueryNodes(); ++j) {
            auto node = queryGraph->getQueryNode(j);
            if (node->getVariableName().empty()) { // Always create anonymous node.
                bindInsertNode(node, result);
                continue;
            }
            if (patternsInScope.contains(node->getVariableName())) {
                continue;
            }
            patternsInScope.insert(node->getVariableName());
            bindInsertNode(node, result);
        }
        for (auto j = 0u; j < queryGraph->getNumQueryRels(); ++j) {
            auto rel = queryGraph->getQueryRel(j);
            if (rel->getVariableName().empty()) { // Always create anonymous rel.
                bindInsertRel(rel, result);
                continue;
            }
            if (patternsInScope.contains(rel->getVariableName())) {
                continue;
            }
            patternsInScope.insert(rel->getVariableName());
            bindInsertRel(rel, result);
        }
    }
    if (result.empty()) {
        throw BinderException("Cannot resolve any node or relationship to create.");
    }
    return result;
}

static void validatePrimaryKeyExistence(const NodeTableCatalogEntry* nodeTableEntry,
    const NodeExpression& node, const expression_vector& defaultExprs) {
    auto primaryKey = nodeTableEntry->getPrimaryKey();
    auto pkeyDefaultExpr = defaultExprs.at(nodeTableEntry->getPrimaryKeyPos());
    if (!node.hasPropertyDataExpr(primaryKey->getName()) &&
        ExpressionUtil::isNullLiteral(*pkeyDefaultExpr)) {
        throw BinderException(
            common::stringFormat("Create node {} expects primary key {} as input.", node.toString(),
                primaryKey->getName()));
    }
}

void Binder::bindInsertNode(std::shared_ptr<NodeExpression> node,
    std::vector<BoundInsertInfo>& infos) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Create node " + node->toString() + " with multiple node labels is not supported.");
    }
    auto catalog = clientContext->getCatalog();
    auto tableID = node->getSingleTableID();
    auto tableSchema = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    KU_ASSERT(tableSchema->getTableType() == TableType::NODE);
    auto insertInfo = BoundInsertInfo(TableType::NODE, node);
    for (auto& entry : catalog->getRdfGraphEntries(clientContext->getTx())) {
        auto rdfEntry = ku_dynamic_cast<CatalogEntry*, RDFGraphCatalogEntry*>(entry);
        if (rdfEntry->isParent(tableID)) {
            insertInfo.conflictAction = ConflictAction::ON_CONFLICT_DO_NOTHING;
        }
    }
    for (auto& expr : node->getPropertyExprs()) {
        auto propertyExpr = ku_dynamic_cast<Expression*, PropertyExpression*>(expr.get());
        if (propertyExpr->hasPropertyID(tableID)) {
            insertInfo.columnExprs.push_back(expr);
        }
    }
    insertInfo.columnDataExprs =
        bindInsertColumnDataExprs(node->getPropertyDataExprRef(), tableSchema->getPropertiesRef());
    validatePrimaryKeyExistence(
        ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableSchema), *node,
        insertInfo.columnDataExprs);
    infos.push_back(std::move(insertInfo));
}

void Binder::bindInsertRel(std::shared_ptr<RelExpression> rel,
    std::vector<BoundInsertInfo>& infos) {
    if (rel->isMultiLabeled() || rel->isBoundByMultiLabeledNode()) {
        throw BinderException(
            "Create rel " + rel->toString() +
            " with multiple rel labels or bound by multiple node labels is not supported.");
    }
    if (ExpressionUtil::isRecursiveRelPattern(*rel)) {
        throw BinderException(
            common::stringFormat("Cannot create recursive rel {}.", rel->toString()));
    }
    rel->setTableIDs(std::vector<common::table_id_t>{rel->getTableIDs()[0]});
    auto relTableID = rel->getSingleTableID();
    auto catalog = clientContext->getCatalog();
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), relTableID);
    TableCatalogEntry* parentTableEntry = nullptr;
    for (auto& rdfGraphEntry : catalog->getRdfGraphEntries(clientContext->getTx())) {
        if (rdfGraphEntry->isParent(relTableID)) {
            parentTableEntry = rdfGraphEntry;
        }
    }
    if (parentTableEntry != nullptr) {
        KU_ASSERT(parentTableEntry->getTableType() == TableType::RDF);
        auto rdfGraphEntry = ku_dynamic_cast<const TableCatalogEntry*, const RDFGraphCatalogEntry*>(
            parentTableEntry);
        if (!rel->hasPropertyDataExpr(std::string(rdf::IRI))) {
            throw BinderException(stringFormat(
                "Insert relationship {} expects {} property as input.", rel->toString(), rdf::IRI));
        }
        // Insert predicate resource node.
        auto resourceTableID = rdfGraphEntry->getResourceTableID();
        auto pNode = createQueryNode(rel->getVariableName(),
            std::vector<common::table_id_t>{resourceTableID});
        auto iriData = rel->getPropertyDataExpr(std::string(rdf::IRI));
        iriData = expressionBinder.bindScalarFunctionExpression(
            expression_vector{std::move(iriData)}, function::ValidatePredicateFunction::name);
        pNode->addPropertyDataExpr(std::string(rdf::IRI), std::move(iriData));
        bindInsertNode(pNode, infos);
        auto nodeInsertInfo = &infos[infos.size() - 1];
        KU_ASSERT(nodeInsertInfo->columnExprs.size() == 1);
        nodeInsertInfo->iriReplaceExpr =
            expressionBinder.bindNodeOrRelPropertyExpression(*rel, std::string(rdf::IRI));
        // Insert triple rel.
        auto relInsertInfo = BoundInsertInfo(TableType::REL, rel);
        std::unordered_map<std::string, std::shared_ptr<Expression>> relPropertyRhsExpr;
        relPropertyRhsExpr.insert({std::string(rdf::PID), pNode->getInternalID()});
        relInsertInfo.columnExprs.push_back(expressionBinder.bindNodeOrRelPropertyExpression(*rel,
            std::string(InternalKeyword::ID)));
        relInsertInfo.columnExprs.push_back(
            expressionBinder.bindNodeOrRelPropertyExpression(*rel, std::string(rdf::PID)));
        relInsertInfo.columnDataExprs =
            bindInsertColumnDataExprs(relPropertyRhsExpr, tableEntry->getPropertiesRef());
        infos.push_back(std::move(relInsertInfo));
    } else {
        auto insertInfo = BoundInsertInfo(TableType::REL, rel);
        insertInfo.columnExprs = rel->getPropertyExprs();
        insertInfo.columnDataExprs = bindInsertColumnDataExprs(rel->getPropertyDataExprRef(),
            tableEntry->getPropertiesRef());
        infos.push_back(std::move(insertInfo));
    }
}

expression_vector Binder::bindInsertColumnDataExprs(
    const std::unordered_map<std::string, std::shared_ptr<Expression>>& propertyRhsExpr,
    const std::vector<Property>& properties) {
    expression_vector result;
    for (auto& property : properties) {
        std::shared_ptr<Expression> rhs;
        if (propertyRhsExpr.contains(property.getName())) {
            rhs = propertyRhsExpr.at(property.getName());
        } else {
            rhs = expressionBinder.bindExpression(*property.getDefaultExpr());
        }
        rhs = expressionBinder.implicitCastIfNecessary(rhs, property.getDataType());
        result.push_back(std::move(rhs));
    }
    return result;
}

std::unique_ptr<BoundUpdatingClause> Binder::bindSetClause(const UpdatingClause& updatingClause) {
    auto& setClause = (SetClause&)updatingClause;
    auto boundSetClause = std::make_unique<BoundSetClause>();
    for (auto& setItem : setClause.getSetItemsRef()) {
        boundSetClause->addInfo(bindSetPropertyInfo(setItem.first.get(), setItem.second.get()));
    }
    return boundSetClause;
}

BoundSetPropertyInfo Binder::bindSetPropertyInfo(parser::ParsedExpression* lhs,
    parser::ParsedExpression* rhs) {
    auto expr = expressionBinder.bindExpression(*lhs->getChild(0));
    auto isNode = ExpressionUtil::isNodePattern(*expr);
    auto isRel = ExpressionUtil::isRelPattern(*expr);
    if (!isNode && !isRel) {
        throw BinderException(
            stringFormat("Cannot set expression {} with type {}. Expect node or rel pattern.",
                expr->toString(), ExpressionTypeUtil::toString(expr->expressionType)));
    }
    auto& patternExpr = expr->constCast<NodeOrRelExpression>();
    auto boundSetItem = bindSetItem(lhs, rhs);
    // Validate not updating tables belong to RDFGraph.
    auto catalog = clientContext->getCatalog();
    for (auto tableID : patternExpr.getTableIDs()) {
        auto tableName = catalog->getTableCatalogEntry(clientContext->getTx(), tableID)->getName();
        for (auto& rdfGraphEntry : catalog->getRdfGraphEntries(clientContext->getTx())) {
            if (rdfGraphEntry->isParent(tableID)) {
                throw BinderException(
                    stringFormat("Cannot set properties of RDFGraph tables. Set {} requires "
                                 "modifying table {} under rdf graph {}.",
                        boundSetItem.first->toString(), tableName, rdfGraphEntry->getName()));
            }
        }
    }
    if (isNode) {
        auto info = BoundSetPropertyInfo(TableType::NODE, expr, boundSetItem);
        auto& property = boundSetItem.first->constCast<PropertyExpression>();
        for (auto id : patternExpr.getTableIDs()) {
            if (property.isPrimaryKey(id)) {
                info.pkExpr = boundSetItem.first;
            }
        }
        return info;
    }
    return BoundSetPropertyInfo(TableType::REL, expr, std::move(boundSetItem));
}

expression_pair Binder::bindSetItem(parser::ParsedExpression* lhs, parser::ParsedExpression* rhs) {
    auto boundLhs = expressionBinder.bindExpression(*lhs);
    auto boundRhs = expressionBinder.bindExpression(*rhs);
    boundRhs = expressionBinder.implicitCastIfNecessary(boundRhs, boundLhs->dataType);
    return make_pair(std::move(boundLhs), std::move(boundRhs));
}

static void validateRdfResourceDeletion(Expression* pattern, main::ClientContext* context) {
    auto node = ku_dynamic_cast<Expression*, NodeExpression*>(pattern);
    for (auto& tableID : node->getTableIDs()) {
        for (auto& rdfGraphEntry : context->getCatalog()->getRdfGraphEntries(context->getTx())) {
            if (rdfGraphEntry->isParent(tableID) &&
                node->hasPropertyExpression(std::string(rdf::IRI))) {
                throw BinderException(
                    stringFormat("Cannot delete node {} because it references to resource "
                                 "node table under RDFGraph {}.",
                        node->toString(), rdfGraphEntry->getName()));
            }
        }
    }
}

std::unique_ptr<BoundUpdatingClause> Binder::bindDeleteClause(
    const UpdatingClause& updatingClause) {
    auto& deleteClause = updatingClause.constCast<DeleteClause>();
    auto deleteType = deleteClause.getDeleteClauseType();
    auto boundDeleteClause = std::make_unique<BoundDeleteClause>();
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto pattern = expressionBinder.bindExpression(*deleteClause.getExpression(i));
        if (ExpressionUtil::isNodePattern(*pattern)) {
            validateRdfResourceDeletion(pattern.get(), clientContext);
            auto deleteNodeInfo = BoundDeleteInfo(deleteType, TableType::NODE, pattern);
            boundDeleteClause->addInfo(std::move(deleteNodeInfo));
        } else if (ExpressionUtil::isRelPattern(*pattern)) {
            if (deleteClause.getDeleteClauseType() == DeleteNodeType::DETACH_DELETE) {
                // TODO(Xiyang): Dummy check here. Make sure this is the correct semantic.
                throw BinderException("Detach delete on rel tables is not supported.");
            }
            auto rel = pattern->constPtrCast<RelExpression>();
            if (rel->getDirectionType() == RelDirectionType::BOTH) {
                throw BinderException("Delete undirected rel is not supported.");
            }
            auto deleteRel = BoundDeleteInfo(deleteType, TableType::REL, pattern);
            boundDeleteClause->addInfo(std::move(deleteRel));
        } else {
            throw BinderException(stringFormat(
                "Cannot delete expression {} with type {}. Expect node or rel pattern.",
                pattern->toString(), ExpressionTypeUtil::toString(pattern->expressionType)));
        }
    }
    return boundDeleteClause;
}

} // namespace binder
} // namespace kuzu
