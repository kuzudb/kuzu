#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_insert_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "catalog/node_table_schema.h"
#include "catalog/rdf_graph_schema.h"
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
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection.getQueryGraph(i);
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
    const TableSchema* tableSchema, const NodeExpression& node) {
    auto nodeTableSchema = ku_dynamic_cast<const TableSchema*, const NodeTableSchema*>(tableSchema);
    auto primaryKey = nodeTableSchema->getPrimaryKey();
    if (*primaryKey->getDataType() == *LogicalType::SERIAL()) {
        return; // No input needed for SERIAL primary key.
    }
    if (!node.hasPropertyDataExpr(primaryKey->getName())) {
        throw BinderException("Create node " + node.toString() + " expects primary key " +
                              primaryKey->getName() + " as input.");
    }
}

void Binder::bindInsertNode(
    std::shared_ptr<NodeExpression> node, std::vector<BoundInsertInfo>& infos) {
    if (node->isMultiLabeled()) {
        throw BinderException(
            "Create node " + node->toString() + " with multiple node labels is not supported.");
    }
    auto tableID = node->getSingleTableID();
    auto tableSchema = catalog.getTableSchema(clientContext->getTx(), tableID);
    KU_ASSERT(tableSchema->getTableType() == TableType::NODE);
    validatePrimaryKeyExistence(tableSchema, *node);
    auto insertInfo = BoundInsertInfo(TableType::NODE, node);
    if (tableSchema->hasParentTableID()) {
        auto parentTableID = tableSchema->getParentTableID();
        auto parentTableSchema = catalog.getTableSchema(clientContext->getTx(), parentTableID);
        KU_ASSERT(parentTableSchema->getTableType() == TableType::RDF);
        insertInfo.conflictAction = ConflictAction::ON_CONFLICT_DO_NOTHING;
    }
    insertInfo.columnExprs = node->getPropertyExprs();
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
    auto tableSchema = catalog.getTableSchema(clientContext->getTx(), relTableID);
    if (tableSchema->hasParentTableID()) {
        auto parentTableID = tableSchema->getParentTableID();
        auto parentTableSchema = catalog.getTableSchema(clientContext->getTx(), parentTableID);
        KU_ASSERT(parentTableSchema->getTableType() == TableType::RDF);
        auto rdfTableSchema =
            ku_dynamic_cast<const TableSchema*, const RdfGraphSchema*>(parentTableSchema);
        // Insert predicate resource node.
        auto resourceTableID = rdfTableSchema->getResourceTableID();
        auto pNode = createQueryNode(
            rel->getVariableName(), std::vector<common::table_id_t>{resourceTableID});
        pNode->addPropertyDataExpr(
            std::string(rdf::IRI), rel->getPropertyDataExpr(std::string(rdf::IRI)));
        bindInsertNode(pNode, infos);
        // Insert triple rel.
        auto relInsertInfo = BoundInsertInfo(TableType::REL, rel);
        std::unordered_map<std::string, std::shared_ptr<Expression>> relPropertyRhsExpr;
        relPropertyRhsExpr.insert({std::string(rdf::PID), pNode->getInternalID()});
        relInsertInfo.columnExprs.push_back(
            rel->getPropertyExpression(std::string(InternalKeyword::ID)));
        relInsertInfo.columnExprs.push_back(rel->getPropertyExpression(std::string(rdf::PID)));
        relInsertInfo.columnDataExprs =
            bindInsertColumnDataExprs(relPropertyRhsExpr, tableSchema->getPropertiesRef());
        infos.push_back(std::move(relInsertInfo));
    } else {
        auto insertInfo = BoundInsertInfo(TableType::REL, rel);
        insertInfo.columnExprs = rel->getPropertyExprs();
        insertInfo.columnDataExprs = bindInsertColumnDataExprs(
            rel->getPropertyDataExprRef(), tableSchema->getPropertiesRef());
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
    auto boundLhs = expressionBinder.bindExpression(*lhs->getChild(0));
    if (ExpressionUtil::isNodePattern(*boundLhs)) {
        return BoundSetPropertyInfo(UpdateTableType::NODE, boundLhs, bindSetItem(lhs, rhs));
    } else if (ExpressionUtil::isRelPattern(*boundLhs)) {
        return BoundSetPropertyInfo(UpdateTableType::REL, boundLhs, bindSetItem(lhs, rhs));
    } else {
        throw BinderException(
            stringFormat("Cannot set expression {} with type {}. Expect node or rel pattern.",
                boundLhs->toString(), expressionTypeToString(boundLhs->expressionType)));
    }
}

expression_pair Binder::bindSetItem(parser::ParsedExpression* lhs, parser::ParsedExpression* rhs) {
    auto boundLhs = expressionBinder.bindExpression(*lhs);
    auto boundRhs = expressionBinder.bindExpression(*rhs);
    boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
    return make_pair(std::move(boundLhs), std::move(boundRhs));
}

std::unique_ptr<BoundUpdatingClause> Binder::bindDeleteClause(
    const UpdatingClause& updatingClause) {
    auto& deleteClause = (DeleteClause&)updatingClause;
    auto boundDeleteClause = std::make_unique<BoundDeleteClause>();
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto nodeOrRel = expressionBinder.bindExpression(*deleteClause.getExpression(i));
        if (ExpressionUtil::isNodePattern(*nodeOrRel)) {
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
