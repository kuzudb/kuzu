#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/query/query_graph_label_analyzer.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_insert_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/assert.h"
#include "common/exception/binder.h"
#include "common/keyword/rdf_keyword.h"
#include "common/string_format.h"
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

static expression_set populateNodeRelScope(const BinderScope& scope) {
    expression_set result;
    for (auto& expression : scope.getExpressions()) {
        if (ExpressionUtil::isNodePattern(*expression) ||
            ExpressionUtil::isRelPattern(*expression)) {
            result.insert(expression);
        }
    }
    return result;
}

std::unique_ptr<BoundUpdatingClause> Binder::bindInsertClause(
    const UpdatingClause& updatingClause) {
    auto& insertClause = (InsertClause&)updatingClause;
    auto nodeRelScope = populateNodeRelScope(*scope);
    // bindGraphPattern will update scope.
    auto boundGraphPattern = bindGraphPattern(insertClause.getPatternElementsRef());
    auto insertInfos = bindInsertInfos(boundGraphPattern.queryGraphCollection, nodeRelScope);
    return std::make_unique<BoundInsertClause>(std::move(insertInfos));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindMergeClause(
    const parser::UpdatingClause& updatingClause) {
    auto& mergeClause = (MergeClause&)updatingClause;
    auto nodeRelScope = populateNodeRelScope(*scope);
    // bindGraphPattern will update scope.
    auto boundGraphPattern = bindGraphPattern(mergeClause.getPatternElementsRef());
    rewriteMatchPattern(boundGraphPattern);
    auto createInfos = bindInsertInfos(boundGraphPattern.queryGraphCollection, nodeRelScope);
    auto boundMergeClause =
        std::make_unique<BoundMergeClause>(std::move(boundGraphPattern.queryGraphCollection),
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

std::vector<BoundInsertInfo> Binder::bindInsertInfos(
    const QueryGraphCollection& queryGraphCollection, const expression_set& nodeRelScope_) {
    auto nodeRelScope = nodeRelScope_;
    std::vector<BoundInsertInfo> result;
    auto analyzer = QueryGraphLabelAnalyzer(*clientContext->getCatalog());
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection.getQueryGraph(i);
        // Ensure query graph does not violate declared schema.
        analyzer.pruneLabel(*queryGraph);
        for (auto j = 0u; j < queryGraph->getNumQueryNodes(); ++j) {
            auto node = queryGraph->getQueryNode(j);
            if (nodeRelScope.contains(node)) {
                continue;
            }
            nodeRelScope.insert(node);
            bindInsertNode(node, result);
        }
        for (auto j = 0u; j < queryGraph->getNumQueryRels(); ++j) {
            auto rel = queryGraph->getQueryRel(j);
            if (nodeRelScope.contains(rel)) {
                continue;
            }
            nodeRelScope.insert(rel);
            bindInsertRel(rel, result);
        }
    }
    if (result.empty()) {
        throw BinderException("Cannot resolve any node or relationship to create.");
    }
    return result;
}

static void validatePrimaryKeyExistence(
    const NodeTableCatalogEntry* nodeTableEntry, const NodeExpression& node) {
    auto primaryKey = nodeTableEntry->getPrimaryKey();
    if (*primaryKey->getDataType() == *LogicalType::SERIAL()) {
        if (node.hasPropertyDataExpr(primaryKey->getName())) {
            throw BinderException(
                common::stringFormat("The primary key of {} is serial, specifying primary key "
                                     "value is not allowed in the create statement.",
                    nodeTableEntry->getName()));
        }
        return; // No input needed for SERIAL primary key.
    }
    if (!node.hasPropertyDataExpr(primaryKey->getName())) {
        throw BinderException(
            common::stringFormat("Create node {} expects primary key {} as input.", node.toString(),
                primaryKey->getName()));
    }
}

void Binder::bindInsertNode(
    std::shared_ptr<NodeExpression> node, std::vector<BoundInsertInfo>& infos) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Create node " + node->toString() + " with multiple node labels is not supported.");
    }
    auto catalog = clientContext->getCatalog();
    auto tableID = node->getSingleTableID();
    auto tableSchema = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    KU_ASSERT(tableSchema->getTableType() == TableType::NODE);
    validatePrimaryKeyExistence(
        ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableSchema), *node);
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
    infos.push_back(std::move(insertInfo));
}

void Binder::bindInsertRel(
    std::shared_ptr<RelExpression> rel, std::vector<BoundInsertInfo>& infos) {
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
        auto pNode = createQueryNode(
            rel->getVariableName(), std::vector<common::table_id_t>{resourceTableID});
        auto iriData = rel->getPropertyDataExpr(std::string(rdf::IRI));
        iriData = expressionBinder.bindScalarFunctionExpression(
            expression_vector{std::move(iriData)}, VALIDATE_PREDICATE_FUNC_NAME);
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
        relInsertInfo.columnExprs.push_back(expressionBinder.bindNodeOrRelPropertyExpression(
            *rel, std::string(InternalKeyword::ID)));
        relInsertInfo.columnExprs.push_back(
            expressionBinder.bindNodeOrRelPropertyExpression(*rel, std::string(rdf::PID)));
        relInsertInfo.columnDataExprs =
            bindInsertColumnDataExprs(relPropertyRhsExpr, tableEntry->getPropertiesRef());
        infos.push_back(std::move(relInsertInfo));
    } else {
        auto insertInfo = BoundInsertInfo(TableType::REL, rel);
        insertInfo.columnExprs = rel->getPropertyExprs();
        insertInfo.columnDataExprs = bindInsertColumnDataExprs(
            rel->getPropertyDataExprRef(), tableEntry->getPropertiesRef());
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
            rhs = expressionBinder.createNullLiteralExpression();
        }
        rhs = ExpressionBinder::implicitCastIfNecessary(rhs, *property.getDataType());
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

BoundSetPropertyInfo Binder::bindSetPropertyInfo(
    parser::ParsedExpression* lhs, parser::ParsedExpression* rhs) {
    auto pattern = expressionBinder.bindExpression(*lhs->getChild(0));
    auto isNode = ExpressionUtil::isNodePattern(*pattern);
    auto isRel = ExpressionUtil::isRelPattern(*pattern);
    if (!isNode && !isRel) {
        throw BinderException(
            stringFormat("Cannot set expression {} with type {}. Expect node or rel pattern.",
                pattern->toString(), expressionTypeToString(pattern->expressionType)));
    }
    auto patternExpr = ku_dynamic_cast<Expression*, NodeOrRelExpression*>(pattern.get());
    auto boundSetItem = bindSetItem(lhs, rhs);
    auto catalog = clientContext->getCatalog();
    for (auto tableID : patternExpr->getTableIDs()) {
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
    return BoundSetPropertyInfo(
        isNode ? UpdateTableType::NODE : UpdateTableType::REL, pattern, std::move(boundSetItem));
}

expression_pair Binder::bindSetItem(parser::ParsedExpression* lhs, parser::ParsedExpression* rhs) {
    auto boundLhs = expressionBinder.bindExpression(*lhs);
    auto boundRhs = expressionBinder.bindExpression(*rhs);
    boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
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
    auto& deleteClause = (DeleteClause&)updatingClause;
    auto boundDeleteClause = std::make_unique<BoundDeleteClause>();
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto nodeOrRel = expressionBinder.bindExpression(*deleteClause.getExpression(i));
        if (ExpressionUtil::isNodePattern(*nodeOrRel)) {
            validateRdfResourceDeletion(nodeOrRel.get(), clientContext);
            auto deleteNodeInfo = BoundDeleteInfo(
                UpdateTableType::NODE, nodeOrRel, deleteClause.getDeleteClauseType());
            boundDeleteClause->addInfo(std::move(deleteNodeInfo));
        } else if (ExpressionUtil::isRelPattern(*nodeOrRel)) {
            if (deleteClause.getDeleteClauseType() == DeleteClauseType::DETACH_DELETE) {
                // TODO(Xiyang): Dummy check here. Make sure this is the correct semantic.
                throw BinderException("Detach delete on rel tables is not supported.");
            }
            auto rel = (RelExpression*)nodeOrRel.get();
            if (rel->getDirectionType() == RelDirectionType::BOTH) {
                throw BinderException("Delete undirected rel is not supported.");
            }
            auto deleteRel =
                BoundDeleteInfo(UpdateTableType::REL, nodeOrRel, DeleteClauseType::DELETE);
            boundDeleteClause->addInfo(std::move(deleteRel));
        } else {
            throw BinderException(stringFormat(
                "Cannot delete expression {} with type {}. Expect node or rel pattern.",
                nodeOrRel->toString(), expressionTypeToString(nodeOrRel->expressionType)));
        }
    }
    return boundDeleteClause;
}

} // namespace binder
} // namespace kuzu
