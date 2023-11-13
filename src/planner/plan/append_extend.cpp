#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_visitor.h"
#include "catalog/rel_table_schema.h"
#include "planner/join_order/cost_model.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_node_label_filter.h"
#include "planner/query_planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;

namespace kuzu {
namespace planner {

static bool extendHasAtMostOneNbrGuarantee(RelExpression& rel, NodeExpression& boundNode,
    ExtendDirection direction, const catalog::Catalog& catalog) {
    if (boundNode.isMultiLabeled()) {
        return false;
    }
    if (rel.isMultiLabeled()) {
        return false;
    }
    if (direction == ExtendDirection::BOTH) {
        return false;
    }
    auto relDirection = ExtendDirectionUtils::getRelDataDirection(direction);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(
        catalog.getReadOnlyVersion()->getTableSchema(rel.getSingleTableID()));
    return relTableSchema->isSingleMultiplicityInDirection(relDirection);
}

static std::unordered_set<table_id_t> getBoundNodeTableIDSet(
    const RelExpression& rel, ExtendDirection extendDirection, const catalog::Catalog& catalog) {
    std::unordered_set<table_id_t> result;
    for (auto tableID : rel.getTableIDs()) {
        auto tableSchema = reinterpret_cast<RelTableSchema*>(
            catalog.getReadOnlyVersion()->getTableSchema(tableID));
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(tableSchema->getBoundTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(tableSchema->getBoundTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(tableSchema->getBoundTableID(RelDataDirection::FWD));
            result.insert(tableSchema->getBoundTableID(RelDataDirection::BWD));
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return result;
}

static std::unordered_set<table_id_t> getNbrNodeTableIDSet(
    const RelExpression& rel, ExtendDirection extendDirection, const catalog::Catalog& catalog) {
    std::unordered_set<table_id_t> result;
    for (auto tableID : rel.getTableIDs()) {
        auto tableSchema = reinterpret_cast<RelTableSchema*>(
            catalog.getReadOnlyVersion()->getTableSchema(tableID));
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(tableSchema->getNbrTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(tableSchema->getNbrTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(tableSchema->getNbrTableID(RelDataDirection::FWD));
            result.insert(tableSchema->getNbrTableID(RelDataDirection::BWD));
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return result;
}

void QueryPlanner::appendNonRecursiveExtend(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    ExtendDirection direction, const expression_vector& properties, LogicalPlan& plan) {
    // Filter bound node label if we know some incoming nodes won't have any outgoing rel. This
    // cannot be done at binding time because the pruning is affected by extend direction.
    auto boundNodeTableIDSet = getBoundNodeTableIDSet(*rel, direction, catalog);
    if (boundNode->getNumTableIDs() > boundNodeTableIDSet.size()) {
        appendNodeLabelFilter(boundNode->getInternalID(), boundNodeTableIDSet, plan);
    }
    // Check for each bound node, can we extend to more than 1 nbr node.
    auto hasAtMostOneNbr = extendHasAtMostOneNbrGuarantee(*rel, *boundNode, direction, catalog);
    // Split properties that cannot be scanned from rel table, specifically RDF predicate IRI.
    expression_vector propertiesToScanFromRelTable;
    std::shared_ptr<Expression> iri;
    for (auto& property : properties) {
        if (reinterpret_cast<PropertyExpression*>(property.get())->isIRI()) {
            iri = property;
            propertiesToScanFromRelTable.push_back(rel->getRdfPredicateInfo()->predicateID);
        } else {
            propertiesToScanFromRelTable.push_back(property);
        }
    }
    // Append extend
    auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel, direction,
        propertiesToScanFromRelTable, hasAtMostOneNbr, plan.getLastOperator());
    appendFlattens(extend->getGroupsPosToFlatten(), plan);
    extend->setChild(0, plan.getLastOperator());
    extend->computeFactorizedSchema();
    // Update cost & cardinality. Note that extend does not change cardinality.
    plan.setCost(CostModel::computeExtendCost(plan));
    if (!hasAtMostOneNbr) {
        auto extensionRate = cardinalityEstimator->getExtensionRate(*rel, *boundNode);
        auto group = extend->getSchema()->getGroup(nbrNode->getInternalID());
        group->setMultiplier(extensionRate);
    }
    plan.setLastOperator(std::move(extend));
    auto nbrNodeTableIDSet = getNbrNodeTableIDSet(*rel, direction, catalog);
    if (nbrNodeTableIDSet.size() > nbrNode->getNumTableIDs()) {
        appendNodeLabelFilter(nbrNode->getInternalID(), nbrNode->getTableIDsSet(), plan);
    }
    if (iri) {
        auto rdfInfo = rel->getRdfPredicateInfo();
        appendFillTableID(rdfInfo->predicateID, rdfInfo->resourceTableIDs[0], plan);
        // Append hash join for remaining properties
        auto tmpPlan = std::make_unique<LogicalPlan>();
        cardinalityEstimator->addNodeIDDom(*rdfInfo->predicateID, rdfInfo->resourceTableIDs);
        appendScanInternalID(rdfInfo->predicateID, rdfInfo->resourceTableIDs, *tmpPlan);
        appendScanNodeProperties(
            rdfInfo->predicateID, rdfInfo->resourceTableIDs, expression_vector{iri}, *tmpPlan);
        appendHashJoin(expression_vector{rdfInfo->predicateID}, JoinType::INNER, plan, *tmpPlan);
    }
}

void QueryPlanner::appendRecursiveExtend(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    ExtendDirection direction, LogicalPlan& plan) {
    auto recursiveInfo = rel->getRecursiveInfo();
    appendAccumulate(AccumulateType::REGULAR, plan);
    // Create recursive plan
    auto recursivePlan = std::make_unique<LogicalPlan>();
    createRecursivePlan(*recursiveInfo, direction, *recursivePlan);
    // Create recursive extend
    if (boundNode->getNumTableIDs() > recursiveInfo->node->getNumTableIDs()) {
        appendNodeLabelFilter(
            boundNode->getInternalID(), recursiveInfo->node->getTableIDsSet(), plan);
    }
    auto extend = std::make_shared<LogicalRecursiveExtend>(boundNode, nbrNode, rel, direction,
        RecursiveJoinType::TRACK_PATH, plan.getLastOperator(), recursivePlan->getLastOperator());
    appendFlattens(extend->getGroupsPosToFlatten(), plan);
    extend->setChild(0, plan.getLastOperator());
    extend->computeFactorizedSchema();
    plan.setLastOperator(extend);
    // Create path node property scan plan
    std::shared_ptr<LogicalOperator> nodeScanRoot;
    if (!recursiveInfo->nodeProjectionList.empty()) {
        auto pathNodePropertyScanPlan = std::make_unique<LogicalPlan>();
        createPathNodePropertyScanPlan(
            recursiveInfo->node, recursiveInfo->nodeProjectionList, *pathNodePropertyScanPlan);
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
            recursiveInfo->rel, direction, relProperties, *pathRelPropertyScanPlan);
        relScanRoot = pathRelPropertyScanPlan->getLastOperator();
        relScanCardinality = pathRelPropertyScanPlan->getCardinality();
    }
    // Create path property probe
    auto pathPropertyProbe = std::make_shared<LogicalPathPropertyProbe>(
        rel, extend, nodeScanRoot, relScanRoot, RecursiveJoinType::TRACK_PATH);
    pathPropertyProbe->computeFactorizedSchema();
    // Check for sip
    auto ratio = plan.getCardinality() / relScanCardinality;
    if (ratio > PlannerKnobs::SIP_RATIO) {
        pathPropertyProbe->setSIP(SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD);
    }
    plan.setLastOperator(std::move(pathPropertyProbe));
    // Update cost
    auto extensionRate = cardinalityEstimator->getExtensionRate(*rel, *boundNode);
    plan.setCost(CostModel::computeRecursiveExtendCost(rel->getUpperBound(), extensionRate, plan));
    // Update cardinality
    auto hasAtMostOneNbr = extendHasAtMostOneNbrGuarantee(*rel, *boundNode, direction, catalog);
    if (!hasAtMostOneNbr) {
        auto group = plan.getSchema()->getGroup(nbrNode->getInternalID());
        group->setMultiplier(extensionRate);
    }
}

static expression_vector collectPropertiesToRead(const std::shared_ptr<Expression>& expression) {
    if (expression == nullptr) {
        return expression_vector{};
    }
    return ExpressionCollector().collectPropertyExpressions(expression);
}

void QueryPlanner::createRecursivePlan(
    const RecursiveInfo& recursiveInfo, ExtendDirection direction, LogicalPlan& plan) {
    auto boundNode = recursiveInfo.node;
    auto nbrNode = recursiveInfo.nodeCopy;
    auto rel = recursiveInfo.rel;
    auto scanFrontier = std::make_shared<LogicalScanFrontier>(
        boundNode->getInternalID(), recursiveInfo.nodePredicateExecFlag);
    scanFrontier->computeFactorizedSchema();
    plan.setLastOperator(std::move(scanFrontier));
    auto nodeProperties = collectPropertiesToRead(recursiveInfo.nodePredicate);
    if (!nodeProperties.empty()) {
        appendScanNodeProperties(boundNode->getInternalID(), boundNode->getTableIDs(),
            ExpressionUtil::removeDuplication(nodeProperties), plan);
    }
    if (recursiveInfo.nodePredicate) {
        appendFilters(recursiveInfo.nodePredicate->splitOnAND(), plan);
    }
    auto relProperties = collectPropertiesToRead(recursiveInfo.relPredicate);
    relProperties.push_back(rel->getInternalIDProperty());
    appendNonRecursiveExtend(
        boundNode, nbrNode, rel, direction, ExpressionUtil::removeDuplication(relProperties), plan);
    if (recursiveInfo.relPredicate) {
        appendFilters(recursiveInfo.relPredicate->splitOnAND(), plan);
    }
}

void QueryPlanner::createPathNodePropertyScanPlan(
    std::shared_ptr<NodeExpression> node, const expression_vector& properties, LogicalPlan& plan) {
    appendScanInternalID(node->getInternalID(), node->getTableIDs(), plan);
    appendScanNodeProperties(node->getInternalID(), node->getTableIDs(), properties, plan);
}

void QueryPlanner::createPathRelPropertyScanPlan(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    ExtendDirection direction, const expression_vector& properties, LogicalPlan& plan) {
    appendScanInternalID(boundNode->getInternalID(), boundNode->getTableIDs(), plan);
    appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, properties, plan);
    appendProjection(properties, plan);
}

void QueryPlanner::appendNodeLabelFilter(std::shared_ptr<Expression> nodeID,
    std::unordered_set<table_id_t> tableIDSet, LogicalPlan& plan) {
    auto filter = std::make_shared<LogicalNodeLabelFilter>(
        std::move(nodeID), std::move(tableIDSet), plan.getLastOperator());
    filter->computeFactorizedSchema();
    plan.setLastOperator(std::move(filter));
}

} // namespace planner
} // namespace kuzu
