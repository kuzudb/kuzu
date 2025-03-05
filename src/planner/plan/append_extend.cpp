#include <utility>

#include "binder/query/reading_clause/bound_gds_call.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/join_type.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "graph/graph_entry.h"
#include "main/client_context.h"
#include "planner/join_order/cost_model.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/extend/recursive_join_type.h"
#include "planner/operator/logical_node_label_filter.h"
#include "planner/operator/logical_path_property_probe.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::transaction;
using namespace kuzu::function;

namespace kuzu {
namespace planner {

static std::unordered_set<table_id_t> getBoundNodeTableIDSet(const RelExpression& rel,
    ExtendDirection extendDirection) {
    std::unordered_set<table_id_t> result;
    for (auto entry : rel.getEntries()) {
        auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(relTableEntry.getBoundTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(relTableEntry.getBoundTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(relTableEntry.getBoundTableID(RelDataDirection::FWD));
            result.insert(relTableEntry.getBoundTableID(RelDataDirection::BWD));
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return result;
}

static std::unordered_set<table_id_t> getNbrNodeTableIDSet(const RelExpression& rel,
    ExtendDirection extendDirection) {
    std::unordered_set<table_id_t> result;
    for (auto entry : rel.getEntries()) {
        auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(relTableEntry.getNbrTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(relTableEntry.getNbrTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(relTableEntry.getNbrTableID(RelDataDirection::FWD));
            result.insert(relTableEntry.getNbrTableID(RelDataDirection::BWD));
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return result;
}

void Planner::appendNonRecursiveExtend(const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode, const std::shared_ptr<RelExpression>& rel,
    ExtendDirection direction, bool extendFromSource, const expression_vector& properties,
    LogicalPlan& plan) {
    // Filter bound node label if we know some incoming nodes won't have any outgoing rel. This
    // cannot be done at binding time because the pruning is affected by extend direction.
    auto boundNodeTableIDSet = getBoundNodeTableIDSet(*rel, direction);
    if (boundNode->getNumEntries() > boundNodeTableIDSet.size()) {
        appendNodeLabelFilter(boundNode->getInternalID(), boundNodeTableIDSet, plan);
    }
    auto properties_ = properties;
    // Append extend
    auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel, direction, extendFromSource,
        properties_, plan.getLastOperator());
    extend->computeFactorizedSchema();
    // Update cost & cardinality. Note that extend does not change factorized cardinality.
    const auto extensionRate =
        cardinalityEstimator.getExtensionRate(*rel, *boundNode, clientContext->getTransaction());
    extend->setCardinality(plan.getLastOperator()->getCardinality());
    plan.setCost(CostModel::computeExtendCost(plan));
    auto group = extend->getSchema()->getGroup(nbrNode->getInternalID());
    group->setMultiplier(extensionRate);
    plan.setLastOperator(std::move(extend));
    auto nbrNodeTableIDSet = getNbrNodeTableIDSet(*rel, direction);
    if (nbrNodeTableIDSet.size() > nbrNode->getNumEntries()) {
        appendNodeLabelFilter(nbrNode->getInternalID(), nbrNode->getTableIDsSet(), plan);
    }
}

void Planner::appendRecursiveExtend(const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode, const std::shared_ptr<RelExpression>& rel,
    ExtendDirection direction, LogicalPlan& plan) {
    // GDS pipeline
    auto recursiveInfo = rel->getRecursiveInfo();
    // Bind graph entry.
    auto graphEntry =
        graph::GraphEntry(recursiveInfo->node->getEntries(), recursiveInfo->rel->getEntries());
    if (recursiveInfo->relPredicate != nullptr) {
        graphEntry.setRelPredicate(recursiveInfo->relPredicate);
    }

    GDSFunction gdsFunction;
    switch (rel->getRelType()) {
    case QueryRelType::VARIABLE_LENGTH_WALK:
    case QueryRelType::VARIABLE_LENGTH_TRAIL:
    case QueryRelType::VARIABLE_LENGTH_ACYCLIC: {
        gdsFunction = VarLenJoinsFunction::getFunction();
    } break;
    case QueryRelType::SHORTEST: {
        gdsFunction = SingleSPPathsFunction::getFunction();
    } break;
    case QueryRelType::ALL_SHORTEST: {
        gdsFunction = AllSPPathsFunction::getFunction();
    } break;
    case QueryRelType::WEIGHTED_SHORTEST: {
        gdsFunction = WeightedSPPathsFunction::getFunction();
    } break;
    case QueryRelType::ALL_WEIGHTED_SHORTEST: {
        gdsFunction = AllWeightedSPPathsFunction::getFunction();
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto semantic = QueryRelTypeUtils::getPathSemantic(rel->getRelType());

    auto bindData = std::make_unique<RJBindData>(graphEntry.copy(), nbrNode);
    bindData->nodeInput = boundNode;
    bindData->lowerBound = recursiveInfo->lowerBound;
    bindData->upperBound = recursiveInfo->upperBound;
    bindData->semantic = semantic;
    bindData->extendDirection = direction;
    // If we extend from right to left, we need to print path in reverse direction.
    bindData->flipPath = *boundNode == *rel->getRightNode();
    if (direction == common::ExtendDirection::BOTH) {
        bindData->directionExpr = recursiveInfo->pathEdgeDirectionsExpr;
    }
    bindData->lengthExpr = recursiveInfo->lengthExpression;
    bindData->pathNodeIDsExpr = recursiveInfo->pathNodeIDsExpr;
    bindData->pathEdgeIDsExpr = recursiveInfo->pathEdgeIDsExpr;
    if (recursiveInfo->weightPropertyExpr != nullptr) {
        bindData->weightPropertyExpr = recursiveInfo->weightPropertyExpr;
        bindData->weightOutputExpr = recursiveInfo->weightOutputExpr;
    }
    gdsFunction.gds->setBindData(std::move(bindData));
    GDSBindInput input;
    auto resultColumns = gdsFunction.gds->getResultColumns(input);
    auto gdsInfo = BoundGDSCallInfo(gdsFunction.copy(), std::move(resultColumns));

    std::shared_ptr<LogicalOperator> nodeMaskRoot = nullptr;
    if (recursiveInfo->nodePredicate != nullptr) {
        auto p = planNodeSemiMask(SemiMaskTargetType::RECURSIVE_EXTEND_PATH_NODE,
            *recursiveInfo->node, recursiveInfo->nodePredicate);
        nodeMaskRoot = p.getLastOperator();
    }
    // E.g. Given schema person-knows->person & person-knows->animal
    // And query MATCH (a:person:animal)-[e*]->(b:person)
    // The destination node b after GDS will contain both person & animal label. We need to prune
    // the animal out.
    common::table_id_set_t nbrTableIDSet;
    auto targetNbrTableIDSet = nbrNode->getTableIDsSet();
    auto recursiveNbrTableIDSet = recursiveInfo->node->getTableIDsSet();
    for (auto& tableID : recursiveNbrTableIDSet) {
        if (targetNbrTableIDSet.contains(tableID)) {
            nbrTableIDSet.insert(tableID);
        }
    }
    if (nbrTableIDSet.size() >= recursiveNbrTableIDSet.size()) {
        nbrTableIDSet.clear(); // No need to prune nbr table id.
    }
    auto probePlan = LogicalPlan();
    auto recursiveExtend =
        std::make_shared<LogicalRecursiveExtend>(gdsInfo.copy(), nbrTableIDSet, nodeMaskRoot);
    recursiveExtend->computeFactorizedSchema();
    probePlan.setLastOperator(std::move(recursiveExtend));
    // Scan path node property pipeline
    std::shared_ptr<LogicalOperator> pathNodePropertyScanRoot = nullptr;
    if (!recursiveInfo->nodeProjectionList.empty()) {
        auto pathNodePropertyScanPlan = LogicalPlan();
        createPathNodePropertyScanPlan(recursiveInfo->node, recursiveInfo->nodeProjectionList,
            pathNodePropertyScanPlan);
        pathNodePropertyScanRoot = pathNodePropertyScanPlan.getLastOperator();
    }
    // Scan path rel property pipeline
    std::shared_ptr<LogicalOperator> pathRelPropertyScanRoot = nullptr;
    if (!recursiveInfo->relProjectionList.empty()) {
        auto pathRelPropertyScanPlan = std::make_unique<LogicalPlan>();
        auto relProperties = recursiveInfo->relProjectionList;
        relProperties.push_back(recursiveInfo->rel->getInternalIDProperty());
        bool extendFromSource = *boundNode == *rel->getSrcNode();
        createPathRelPropertyScanPlan(recursiveInfo->node, recursiveInfo->nodeCopy,
            recursiveInfo->rel, direction, extendFromSource, relProperties,
            *pathRelPropertyScanPlan);
        pathRelPropertyScanRoot = pathRelPropertyScanPlan->getLastOperator();
    }
    // Construct path by probing scanned properties
    auto pathPropertyProbe =
        std::make_shared<LogicalPathPropertyProbe>(rel, probePlan.getLastOperator(),
            pathNodePropertyScanRoot, pathRelPropertyScanRoot, RecursiveJoinType::TRACK_PATH);
    pathPropertyProbe->direction = direction;
    pathPropertyProbe->extendFromLeft = *boundNode == *rel->getLeftNode();
    pathPropertyProbe->pathNodeIDs = recursiveInfo->pathNodeIDsExpr;
    pathPropertyProbe->pathEdgeIDs = recursiveInfo->pathEdgeIDsExpr;
    pathPropertyProbe->computeFactorizedSchema();
    auto extensionRate =
        cardinalityEstimator.getExtensionRate(*rel, *boundNode, clientContext->getTransaction());
    auto resultCard =
        cardinalityEstimator.multiply(extensionRate, plan.getLastOperator()->getCardinality());
    pathPropertyProbe->setCardinality(resultCard);
    probePlan.setLastOperator(pathPropertyProbe);
    probePlan.setCost(plan.getCardinality());

    // Join with input node
    auto joinConditions = expression_vector{boundNode->getInternalID()};
    appendHashJoin(joinConditions, JoinType::INNER, probePlan, plan, plan);
    // Hash join above is joining input node with its properties. So 1-1 match is guaranteed and
    // thus should not change cardinality.
    plan.getLastOperator()->setCardinality(resultCard);
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
