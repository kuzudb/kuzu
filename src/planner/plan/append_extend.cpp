#include <utility>

#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_visitor.h"
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
#include "planner/operator/logical_gds_call.h"
#include "planner/operator/logical_node_label_filter.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/operator/sip/logical_semi_masker.h"
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
    // Update cost & cardinality. Note that extend does not change cardinality.
    plan.setCost(CostModel::computeExtendCost(plan));
    auto extensionRate =
        cardinalityEstimator.getExtensionRate(*rel, *boundNode, clientContext->getTx());
    auto group = extend->getSchema()->getGroup(nbrNode->getInternalID());
    group->setMultiplier(extensionRate);
    plan.setLastOperator(std::move(extend));
    auto nbrNodeTableIDSet = getNbrNodeTableIDSet(*rel, direction);
    if (nbrNodeTableIDSet.size() > nbrNode->getNumEntries()) {
        appendNodeLabelFilter(nbrNode->getInternalID(), nbrNode->getTableIDsSet(), plan);
    }
}

void Planner::appendRecursiveExtendAsGDS(const std::shared_ptr<NodeExpression>& boundNode,
    const std::shared_ptr<NodeExpression>& nbrNode, const std::shared_ptr<RelExpression>& rel,
    ExtendDirection direction, LogicalPlan& plan) {
    // GDS pipeline
    auto recursiveInfo = rel->getRecursiveInfo();
    auto graphEntry =
        graph::GraphEntry(recursiveInfo->node->getEntries(), recursiveInfo->rel->getEntries());
    if (recursiveInfo->relPredicate != nullptr) {
        graphEntry.setRelPredicate(recursiveInfo->relPredicate);
    }
    GDSFunction gdsFunction;
    switch (rel->getRelType()) {
    case common::QueryRelType::VARIABLE_LENGTH_WALK:
    case common::QueryRelType::VARIABLE_LENGTH_TRAIL:
    case common::QueryRelType::VARIABLE_LENGTH_ACYCLIC: {
        gdsFunction = VarLenJoinsFunction::getFunction();
    } break;
    case QueryRelType::SHORTEST: {
        gdsFunction = SingleSPPathsFunction::getFunction();
    } break;
    case QueryRelType::ALL_SHORTEST: {
        gdsFunction = AllSPPathsFunction::getFunction();
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto semantic = QueryRelTypeUtils::getPathSemantic(rel->getRelType());
    auto bindData = std::make_unique<RJBindData>(boundNode, nbrNode, recursiveInfo->lowerBound,
        recursiveInfo->upperBound, semantic, direction);
    // If we extend from right to left, we need to print path in reverse direction.
    bindData->flipPath = *boundNode == *rel->getRightNode();
    if (direction == common::ExtendDirection::BOTH) {
        bindData->directionExpr = recursiveInfo->pathEdgeDirectionsExpr;
    }
    bindData->lengthExpr = recursiveInfo->lengthExpression;
    bindData->pathNodeIDsExpr = recursiveInfo->pathNodeIDsExpr;
    bindData->pathEdgeIDsExpr = recursiveInfo->pathEdgeIDsExpr;
    gdsFunction.gds->setBindData(std::move(bindData));
    auto resultColumns = gdsFunction.gds->getResultColumns(nullptr /* binder*/);
    auto gdsInfo =
        BoundGDSCallInfo(gdsFunction.copy(), graphEntry.copy(), std::move(resultColumns));
    auto probePlan = LogicalPlan();
    auto gdsCall = getGDSCall(gdsInfo);
    gdsCall->computeFactorizedSchema();
    if (recursiveInfo->nodePredicate != nullptr) {
        auto p = LogicalPlan();
        createPathNodeFilterPlan(recursiveInfo->node, recursiveInfo->originalNodePredicate, p);
        gdsCall->ptrCast<LogicalGDSCall>()->setNodePredicateRoot(p.getLastOperator());
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
    if (nbrTableIDSet.size() < recursiveNbrTableIDSet.size()) {
        gdsCall->ptrCast<LogicalGDSCall>()->setNbrTableIDSet(nbrTableIDSet);
    }
    probePlan.setLastOperator(std::move(gdsCall));
    // Scan path node property pipeline
    std::shared_ptr<LogicalOperator> pathNodePropertyScanRoot = nullptr;
    if (!recursiveInfo->nodeProjectionList.empty()) {
        auto pathNodePropertyScanPlan = LogicalPlan();
        createPathNodePropertyScanPlan(recursiveInfo->node, recursiveInfo->nodeProjectionList,
            pathNodePropertyScanPlan);
        pathNodePropertyScanRoot = pathNodePropertyScanPlan.getLastOperator();
    }
    // Scan path rel property pipeline
    std::shared_ptr<LogicalOperator> pathRelPropertyScanRoot;
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
    probePlan.setLastOperator(pathPropertyProbe);
    probePlan.setCost(plan.getCardinality());
    auto extensionRate =
        cardinalityEstimator.getExtensionRate(*rel, *boundNode, clientContext->getTx());
    probePlan.setCardinality(plan.getCardinality() * extensionRate);
    // Join with input node
    auto joinConditions = expression_vector{boundNode->getInternalID()};
    appendHashJoin(joinConditions, JoinType::INNER, probePlan, plan, plan);
    // Hash join above should not change the cardinality of probe plan.
    plan.setCardinality(probePlan.getCardinality());
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
    if (boundNode->getNumEntries() > recursiveInfo->node->getNumEntries()) {
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
        pathPropertyProbe->getSIPInfoUnsafe().position = SemiMaskPosition::PROHIBIT_PROBE_TO_BUILD;
    }
    plan.setLastOperator(std::move(pathPropertyProbe));
    // Update cost
    auto extensionRate =
        cardinalityEstimator.getExtensionRate(*rel, *boundNode, clientContext->getTx());
    plan.setCost(CostModel::computeRecursiveExtendCost(rel->getUpperBound(), extensionRate, plan));
    // Update cardinality
    auto group = plan.getSchema()->getGroup(nbrNode->getInternalID());
    group->setMultiplier(extensionRate);
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
    appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, extendFromSource,
        ExpressionUtil::removeDuplication(relProperties), plan);
    if (recursiveInfo.relPredicate) {
        appendFilters(recursiveInfo.relPredicate->splitOnAND(), plan);
    }
}

void Planner::createPathNodeFilterPlan(const std::shared_ptr<NodeExpression>& node,
    std::shared_ptr<Expression> nodePredicate, LogicalPlan& plan) {
    auto collector = PropertyExprCollector();
    collector.visit(nodePredicate);
    auto properties = ExpressionUtil::removeDuplication(collector.getPropertyExprs());
    appendScanNodeTable(node->getInternalID(), node->getTableIDs(), properties, plan);
    appendFilter(nodePredicate, plan);
    auto semiMasker = std::make_shared<LogicalSemiMasker>(SemiMaskKeyType::NODE,
        SemiMaskTargetType::GDS_PATH_NODE, node->getInternalID(), node->getTableIDs(),
        plan.getLastOperator());
    semiMasker->computeFlatSchema();
    plan.setLastOperator(semiMasker);
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
