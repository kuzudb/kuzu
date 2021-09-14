#include "test/planner/planner_test_helper.h"

#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"

class PropertyScanPushDownTest : public PlannerTest {};

/**
 * Assume optimizer picks QVO: b, a
 */
TEST_F(PropertyScanPushDownTest, FilterPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = b.age RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto& op =
        *plan->lastOperator->prevOperator->prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op.getLogicalOperatorType());
    auto& scanNodeProperty = (LogicalScanNodeProperty&)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty.nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

/**
 * Assume optimizer picks QVO: b, a
 */
TEST_F(PropertyScanPushDownTest, ProjectionPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) RETURN a.age, b.age";
    auto plan = getBestPlan(query);
    auto& op = *plan->lastOperator->prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op.getLogicalOperatorType());
    auto& scanNodeProperty = (LogicalScanNodeProperty&)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty.nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

TEST_F(PropertyScanPushDownTest, FilterAndProjectionPropertyPushDownTest) {
    auto query = "MATCH (a:person) WHERE a.age = 2 RETURN a.name";
    auto plan = getBestPlan(query);
    auto& op1 = *plan->lastOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op1.getLogicalOperatorType());
    auto& op2 = *op1.prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op2.getLogicalOperatorType());
}

// TODO: remove tests below once end to end test for aggregation is added
TEST_F(PropertyScanPushDownTest, AggregationTest) {
    auto query = "MATCH (a:person) RETURN SUM(a.age)";
    auto plan = getBestPlan(query);
    auto& op1 = *plan->lastOperator;
    ASSERT_EQ(LOGICAL_AGGREGATE, op1.getLogicalOperatorType());
    auto& aggregate = (LogicalAggregate&)op1;
    auto aggExpr = aggregate.getExpressionsToAggregate()[0];
    ASSERT_EQ(aggExpr->expressionType, SUM_FUNC);
    ASSERT_EQ(plan->schema->getGroupPos(aggExpr->getInternalName()), 0);
    ASSERT_EQ(
        aggregate.getSchemaBeforeAggregate()->getGroupPos(aggExpr->children[0]->getInternalName()),
        0);
}
