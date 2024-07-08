#include <utility>

#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_visitor.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/join_type.h"
#include "main/client_context.h"
#include "planner/join_order/cost_model.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_node_label_filter.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace planner {

static bool extendHasAtMostOneNbrGuarantee(RelExpression& rel, NodeExpression& boundNode,
    ExtendDirection direction, const main::ClientContext& clientContext) {
    if (rel.isEmpty()) {
        return false;
    }
    if (boundNode.isMultiLabeled()) {
        return false;
    }
    if (rel.isMultiLabeled()) {
        return false;
    }
    if (direction == ExtendDirection::BOTH) {
        return false;
    }
    auto relDirection = ExtendDirectionUtil::getRelDataDirection(direction);
    auto catalog = clientContext.getCatalog();
    auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
        catalog->getTableCatalogEntry(clientContext.getTx(), rel.getSingleTableID()));
    return relTableEntry->isSingleMultiplicity(relDirection);
}

static std::unordered_set<table_id_t> getBoundNodeTableIDSet(const RelExpression& rel,
    ExtendDirection extendDirection, const main::ClientContext& clientContext) {
    std::unordered_set<table_id_t> result;
    auto catalog = clientContext.getCatalog();
    for (auto tableID : rel.getTableIDs()) {
        auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            catalog->getTableCatalogEntry(clientContext.getTx(), tableID));
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(relTableEntry->getBoundTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(relTableEntry->getBoundTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(relTableEntry->getBoundTableID(RelDataDirection::FWD));
            result.insert(relTableEntry->getBoundTableID(RelDataDirection::BWD));
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return result;
}

static std::unordered_set<table_id_t> getNbrNodeTableIDSet(const RelExpression& rel,
    ExtendDirection extendDirection, const main::ClientContext& clientContext) {
    std::unordered_set<table_id_t> result;
    auto catalog = clientContext.getCatalog();
    for (auto tableID : rel.getTableIDs()) {
        auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            catalog->getTableCatalogEntry(clientContext.getTx(), tableID));
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(relTableEntry->getNbrTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(relTableEntry->getNbrTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(relTableEntry->getNbrTableID(RelDataDirection::FWD));
            result.insert(relTableEntry->getNbrTableID(RelDataDirection::BWD));
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return result;
}

static std::shared_ptr<Expression> getIRIProperty(const expression_vector& properties) {
    for (auto& property : properties) {
        auto propertyExpr = ku_dynamic_cast<Expression*, PropertyExpression*>(property.get());
        if (propertyExpr->isIRI()) {
            return property;
        }
    }
    return nullptr;
}

static void validatePropertiesContainRelID(const RelExpression& rel,
    const expression_vector& properties) {
    if (rel.isEmpty()) {
        return;
    }
    for (auto& property : properties) {
        if (*property == *rel.getInternalIDProperty()) {
            return;
        }
    }
    // LCOV_EXCL_START
    throw RuntimeException(stringFormat(
        "Internal ID of relationship {} is not scanned. This should not happen.", rel.toString()));
    // LCOV_EXCL_STOP
}

void Planner::appendNonRecursiveExtend(const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode, const std::shared_ptr<RelExpression>& rel,
    ExtendDirection direction, bool extendFromSource, const expression_vector& properties,
    LogicalPlan& plan) {
    validatePropertiesContainRelID(*rel, properties);
    // Filter bound node label if we know some incoming nodes won't have any outgoing rel. This
    // cannot be done at binding time because the pruning is affected by extend direction.
    auto boundNodeTableIDSet = getBoundNodeTableIDSet(*rel, direction, *clientContext);
    if (boundNode->getNumTableIDs() > boundNodeTableIDSet.size()) {
        appendNodeLabelFilter(boundNode->getInternalID(), boundNodeTableIDSet, plan);
    }
    // Check for each bound node, can we extend to more than 1 nbr node.
    auto hasAtMostOneNbr =
        extendHasAtMostOneNbrGuarantee(*rel, *boundNode, direction, *clientContext);
    auto properties_ = properties;
    auto iri = getIRIProperty(properties);
    if (iri != nullptr) {
        // IRI cannot be scanned directly from rel table. Instead, we scan PID.
        properties_.push_back(rel->getRdfPredicateInfo()->predicateID);
        // Remove duplicate predicate ID.
        properties_ = ExpressionUtil::removeDuplication(properties_);
    }
    // Append extend
    auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel, direction, extendFromSource,
        properties_, hasAtMostOneNbr, plan.getLastOperator());
    appendFlattens(extend->getGroupsPosToFlatten(), plan);
    extend->setChild(0, plan.getLastOperator());
    extend->computeFactorizedSchema();
    // Update cost & cardinality. Note that extend does not change cardinality.
    plan.setCost(CostModel::computeExtendCost(plan));
    auto extensionRate =
        cardinalityEstimator.getExtensionRate(*rel, *boundNode, clientContext->getTx());
    auto group = extend->getSchema()->getGroup(nbrNode->getInternalID());
    group->setMultiplier(extensionRate);
    plan.setLastOperator(std::move(extend));
    auto nbrNodeTableIDSet = getNbrNodeTableIDSet(*rel, direction, *clientContext);
    if (nbrNodeTableIDSet.size() > nbrNode->getNumTableIDs()) {
        appendNodeLabelFilter(nbrNode->getInternalID(), nbrNode->getTableIDsSet(), plan);
    }
    if (iri) {
        // Use additional hash join to convert PID to IRI.
        auto rdfInfo = rel->getRdfPredicateInfo();
        // Append hash join for remaining properties
        auto tmpPlan = std::make_unique<LogicalPlan>();
        cardinalityEstimator.addNodeIDDom(*rdfInfo->predicateID, rdfInfo->resourceTableIDs,
            clientContext->getTx());
        appendScanNodeTable(rdfInfo->predicateID, rdfInfo->resourceTableIDs, expression_vector{iri},
            *tmpPlan);
        appendHashJoin(expression_vector{rdfInfo->predicateID}, JoinType::INNER, plan, *tmpPlan,
            plan);
    }
}

void Planner::appendRecursiveExtend(const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode, const std::shared_ptr<RelExpression>& rel,
    ExtendDirection direction, LogicalPlan& plan) {
    auto recursiveInfo = rel->getRecursiveInfo();
    appendAccumulate(plan);
    // Create recursive plan
    auto recursivePlan = std::make_unique<LogicalPlan>();
    bool extendFromSource = *boundNode == *rel->getSrcNode();
    createRecursivePlan(*recursiveInfo, direction, extendFromSource, *recursivePlan);
    // Create recursive extend
    if (boundNode->getNumTableIDs() > recursiveInfo->node->getNumTableIDs()) {
        appendNodeLabelFilter(boundNode->getInternalID(), recursiveInfo->node->getTableIDsSet(),
            plan);
    }
    auto extend = std::make_shared<LogicalRecursiveExtend>(boundNode, nbrNode, rel, direction,
        extendFromSource, RecursiveJoinType::TRACK_PATH, plan.getLastOperator(),
        recursivePlan->getLastOperator());
    appendFlattens(extend->getGroupsPosToFlatten(), plan);
    extend->setChild(0, plan.getLastOperator());
    extend->computeFactorizedSchema();
    plan.setLastOperator(extend);
    // Create path node property scan plan
    std::shared_ptr<LogicalOperator> nodeScanRoot;
    if (!recursiveInfo->nodeProjectionList.empty()) {
        auto pathNodePropertyScanPlan = std::make_unique<LogicalPlan>();
        createPathNodePropertyScanPlan(recursiveInfo->node, recursiveInfo->nodeProjectionList,
            *pathNodePropertyScanPlan);
        nodeScanRoot = pathNodePropertyScanPlan->getLastOperator();
    }
    // Create path rel property scan plan
    uint64_t relScanCardinality = 1u;
    std::shared_ptr<LogicalOperator> relScanRoot;
    if (!recursiveInfo->relProjectionList.empty()) {
        auto pathRelPropertyScanPlan = std::make_unique<LogicalPlan>();
        auto relProperties = recursiveInfo->relProjectionList;
        relProperties.push_back(recursiveInfo->rel->getInternalIDProperty());
        createPathRelPropertyScanPlan(recursiveInfo->node, recursiveInfo->nodeCopy,
            recursiveInfo->rel, direction, extendFromSource, relProperties,
            *pathRelPropertyScanPlan);
        relScanRoot = pathRelPropertyScanPlan->getLastOperator();
        relScanCardinality = pathRelPropertyScanPlan->getCardinality();
    }
    // Create path property probe
    auto pathPropertyProbe = std::make_shared<LogicalPathPropertyProbe>(rel, extend, nodeScanRoot,
        relScanRoot, RecursiveJoinType::TRACK_PATH);
    pathPropertyProbe->computeFactorizedSchema();
    // Check for sip
    auto ratio = plan.getCardinality() / relScanCardinality;
    if (ratio > PlannerKnobs::SIP_RATIO) {
        pathPropertyProbe->getSIPInfoUnsafe().position = SemiMaskPosition::PROHIBIT;
    }
    plan.setLastOperator(std::move(pathPropertyProbe));
    // Update cost
    auto extensionRate =
        cardinalityEstimator.getExtensionRate(*rel, *boundNode, clientContext->getTx());
    plan.setCost(CostModel::computeRecursiveExtendCost(rel->getUpperBound(), extensionRate, plan));
    // Update cardinality
    auto hasAtMostOneNbr =
        extendHasAtMostOneNbrGuarantee(*rel, *boundNode, direction, *clientContext);
    if (!hasAtMostOneNbr) {
        auto group = plan.getSchema()->getGroup(nbrNode->getInternalID());
        group->setMultiplier(extensionRate);
    }
}

static expression_vector collectPropertiesToRead(const std::shared_ptr<Expression>& expression) {
    if (expression == nullptr) {
        return expression_vector{};
    }
    auto collector = PropertyExprCollector();
    collector.visit(expression);
    return collector.getPropertyExprs();
}

void Planner::createRecursivePlan(const RecursiveInfo& recursiveInfo, ExtendDirection direction,
    bool extendFromSource, LogicalPlan& plan) {
    auto boundNode = recursiveInfo.node;
    auto nbrNode = recursiveInfo.nodeCopy;
    auto rel = recursiveInfo.rel;
    auto nodeProperties = collectPropertiesToRead(recursiveInfo.nodePredicate);
    appendScanNodeTable(boundNode->getInternalID(), boundNode->getTableIDs(),
        ExpressionUtil::removeDuplication(nodeProperties), plan);
    auto& scan = plan.getLastOperator()->cast<LogicalScanNodeTable>();
    scan.setScanType(LogicalScanNodeTableType::OFFSET_SCAN);
    scan.setExtraInfo(std::make_unique<RecursiveJoinScanInfo>(recursiveInfo.nodePredicateExecFlag));
    scan.computeFactorizedSchema();
    if (recursiveInfo.nodePredicate) {
        appendFilters(recursiveInfo.nodePredicate->splitOnAND(), plan);
    }
    auto relProperties = collectPropertiesToRead(recursiveInfo.relPredicate);
    relProperties.push_back(rel->getInternalIDProperty());
    auto iri = getIRIProperty(relProperties);
    if (iri != nullptr) {
        // IRI Cannot be scanned directly from rel table. For recursive plan filter, we first read
        // PID, then lookup resource node table to convert PID to IRI.
        relProperties = ExpressionUtil::excludeExpression(relProperties, *iri);
        relProperties.push_back(rel->getRdfPredicateInfo()->predicateID);
        appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, extendFromSource,
            ExpressionUtil::removeDuplication(relProperties), plan);
        auto rdfInfo = rel->getRdfPredicateInfo();
        appendScanNodeTable(rdfInfo->predicateID, rdfInfo->resourceTableIDs, expression_vector{iri},
            plan);
    } else {
        appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, extendFromSource,
            ExpressionUtil::removeDuplication(relProperties), plan);
    }
    if (recursiveInfo.relPredicate) {
        appendFilters(recursiveInfo.relPredicate->splitOnAND(), plan);
    }
}

void Planner::createPathNodePropertyScanPlan(const std::shared_ptr<NodeExpression>& node,
    const expression_vector& properties, LogicalPlan& plan) {
    appendScanNodeTable(node->getInternalID(), node->getTableIDs(), properties, plan);
}

void Planner::createPathRelPropertyScanPlan(const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode, const std::shared_ptr<RelExpression>& rel,
    ExtendDirection direction, bool extendFromSource, const expression_vector& properties,
    LogicalPlan& plan) {
    appendScanNodeTable(boundNode->getInternalID(), boundNode->getTableIDs(), {}, plan);
    appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, extendFromSource, properties,
        plan);
    appendProjection(properties, plan);
}

void Planner::appendNodeLabelFilter(std::shared_ptr<Expression> nodeID,
    std::unordered_set<table_id_t> tableIDSet, LogicalPlan& plan) {
    auto filter = std::make_shared<LogicalNodeLabelFilter>(std::move(nodeID), std::move(tableIDSet),
        plan.getLastOperator());
    filter->computeFactorizedSchema();
    plan.setLastOperator(std::move(filter));
}

} // namespace planner
} // namespace kuzu
