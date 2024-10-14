#include "planner/join_order/cardinality_estimator.h"

#include "binder/expression/property_expression.h"
#include "main/client_context.h"
#include "planner/join_order/join_order_util.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace planner {

void CardinalityEstimator::initNodeIDDom(const Transaction* transaction,
    const QueryGraph& queryGraph) {
    for (auto i = 0u; i < queryGraph.getNumQueryNodes(); ++i) {
        auto node = queryGraph.getQueryNode(i).get();
        addNodeIDDomAndStats(transaction, *node->getInternalID(), node->getTableIDs());
    }
    for (auto i = 0u; i < queryGraph.getNumQueryRels(); ++i) {
        auto rel = queryGraph.getQueryRel(i);
        if (QueryRelTypeUtils::isRecursive(rel->getRelType())) {
            auto node = rel->getRecursiveInfo()->node.get();
            addNodeIDDomAndStats(transaction, *node->getInternalID(), node->getTableIDs());
        }
    }
}

void CardinalityEstimator::addNodeIDDomAndStats(const Transaction* transaction,
    const binder::Expression& nodeID, const std::vector<common::table_id_t>& tableIDs) {
    auto key = nodeID.getUniqueName();
    auto numNodes = 0u;
    for (auto tableID : tableIDs) {
        auto stats =
            context->getStorageManager()->getTable(tableID)->cast<storage::NodeTable>().getStats(
                transaction);
        numNodes += stats.getTableCard();
        if (!nodeTableStats.contains(tableID)) {
            nodeTableStats.insert({tableID, std::move(stats)});
        }
    }
    if (!nodeIDName2dom.contains(key)) {
        nodeIDName2dom.insert({key, numNodes});
    }
}

uint64_t CardinalityEstimator::estimateScanNode(LogicalOperator* op) {
    auto& scan = op->constCast<LogicalScanNodeTable>();
    return atLeastOne(getNodeIDDom(scan.getNodeID()->getUniqueName()));
}

cardinality_t CardinalityEstimator::estimateExtend(double extensionRate,
    const LogicalPlan& childPlan) {
    return atLeastOne(extensionRate * childPlan.getCardinality());
}

uint64_t CardinalityEstimator::estimateHashJoin(const expression_vector& joinKeys,
    const LogicalPlan& probePlan, const LogicalPlan& buildPlan) {
    auto denominator = 1u;
    for (auto& joinKey : joinKeys) {
        // TODO(Xiyang): we should be able to estimate non-ID-based joins as well.
        if (nodeIDName2dom.contains(joinKey->getUniqueName())) {
            denominator *= getNodeIDDom(joinKey->getUniqueName());
        }
    }
    return atLeastOne(probePlan.estCardinality *
                      JoinOrderUtil::getJoinKeysFlatCardinality(joinKeys, buildPlan) /
                      atLeastOne(denominator));
}

uint64_t CardinalityEstimator::estimateCrossProduct(const LogicalPlan& probePlan,
    const LogicalPlan& buildPlan) {
    return atLeastOne(probePlan.estCardinality * buildPlan.estCardinality);
}

uint64_t CardinalityEstimator::estimateIntersect(const expression_vector& joinNodeIDs,
    const LogicalPlan& probePlan, const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
    // Formula 1: treat intersect as a Filter on probe side.
    uint64_t estCardinality1 =
        probePlan.estCardinality * PlannerKnobs::NON_EQUALITY_PREDICATE_SELECTIVITY;
    // Formula 2: assume independence on join conditions.
    auto denominator = 1u;
    for (auto& joinNodeID : joinNodeIDs) {
        denominator *= getNodeIDDom(joinNodeID->getUniqueName());
    }
    auto numerator = probePlan.estCardinality;
    for (auto& buildPlan : buildPlans) {
        numerator *= buildPlan->estCardinality;
    }
    auto estCardinality2 = numerator / atLeastOne(denominator);
    // Pick minimum between the two formulas.
    return atLeastOne(std::min<uint64_t>(estCardinality1, estCardinality2));
}

uint64_t CardinalityEstimator::estimateFlatten(const LogicalPlan& childPlan,
    f_group_pos groupPosToFlatten) {
    auto group = childPlan.getSchema()->getGroup(groupPosToFlatten);
    return atLeastOne(childPlan.estCardinality * group->cardinalityMultiplier);
}

static bool isPrimaryKey(const Expression& expression) {
    if (expression.expressionType != ExpressionType::PROPERTY) {
        return false;
    }
    return ((PropertyExpression&)expression).isPrimaryKey();
}

static bool isSingleLabelledProperty(const Expression& expression) {
    if (expression.expressionType != ExpressionType::PROPERTY) {
        return false;
    }
    return expression.constCast<PropertyExpression>().isSingleLabel();
}

uint64_t CardinalityEstimator::estimateFilter(const LogicalPlan& childPlan,
    const Expression& predicate) {
    if (predicate.expressionType == ExpressionType::EQUALS) {
        if (isPrimaryKey(*predicate.getChild(0)) || isPrimaryKey(*predicate.getChild(1))) {
            return 1;
        } else {
            KU_ASSERT(predicate.getNumChildren() >= 1);
            if (isSingleLabelledProperty(*predicate.getChild(0))) {
                auto& propertyExpr = predicate.getChild(0)->cast<PropertyExpression>();
                auto tableID = propertyExpr.getSingleTableID();
                if (nodeTableStats.contains(tableID)) {
                    auto columnID = propertyExpr.getColumnID(
                        *context->getCatalog()->getTableCatalogEntry(context->getTx(), tableID));
                    if (columnID != INVALID_COLUMN_ID && columnID != ROW_IDX_COLUMN_ID) {
                        auto& stats = nodeTableStats.at(tableID);
                        auto numDistinctValues = atLeastOne(stats.getNumDistinctValues(columnID));
                        return atLeastOne(childPlan.estCardinality / numDistinctValues);
                    }
                }
            }
            return atLeastOne(
                childPlan.estCardinality * PlannerKnobs::EQUALITY_PREDICATE_SELECTIVITY);
        }
    } else {
        return atLeastOne(
            childPlan.estCardinality * PlannerKnobs::NON_EQUALITY_PREDICATE_SELECTIVITY);
    }
}

uint64_t CardinalityEstimator::getNumNodes(const Transaction*,
    const std::vector<table_id_t>& tableIDs) {
    auto numNodes = 0u;
    for (auto& tableID : tableIDs) {
        KU_ASSERT(nodeTableStats.contains(tableID));
        numNodes += nodeTableStats.at(tableID).getTableCard();
    }
    return atLeastOne(numNodes);
}

uint64_t CardinalityEstimator::getNumRels(const Transaction* transaction,
    const std::vector<table_id_t>& tableIDs) {
    auto numRels = 0u;
    for (auto tableID : tableIDs) {
        numRels += context->getStorageManager()->getTable(tableID)->getNumTotalRows(transaction);
    }
    return atLeastOne(numRels);
}

double CardinalityEstimator::getExtensionRate(const RelExpression& rel,
    const NodeExpression& boundNode, const Transaction* transaction) {
    auto numBoundNodes = static_cast<double>(getNumNodes(transaction, boundNode.getTableIDs()));
    auto numRels = static_cast<double>(getNumRels(transaction, rel.getTableIDs()));
    KU_ASSERT(numBoundNodes > 0);
    auto oneHopExtensionRate = numRels / atLeastOne(numBoundNodes);
    switch (rel.getRelType()) {
    case QueryRelType::NON_RECURSIVE: {
        return oneHopExtensionRate;
    }
    case QueryRelType::VARIABLE_LENGTH_WALK:
    case QueryRelType::VARIABLE_LENGTH_TRAIL:
    case QueryRelType::VARIABLE_LENGTH_ACYCLIC: {
        auto rate = oneHopExtensionRate * std::max<uint16_t>(rel.getUpperBound(), 1);
        return rate * context->getClientConfig()->recursivePatternCardinalityScaleFactor;
    }
    case QueryRelType::SHORTEST:
    case QueryRelType::ALL_SHORTEST: {
        auto rate = std::min<double>(
            oneHopExtensionRate * std::max<uint16_t>(rel.getUpperBound(), 1), numRels);
        return rate * context->getClientConfig()->recursivePatternCardinalityScaleFactor;
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace planner
} // namespace kuzu
