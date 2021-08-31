#include "test/planner/planner_test_helper.h"

#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"

class PropertyScanPushDownTest : public PlannerTest {};

/**
 * Assume optimizer picks QVO: b, a
 */
TEST_F(PropertyScanPushDownTest, FilterPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = b.age RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto& op = *plan->lastOperator->prevOperator->prevOperator->prevOperator->prevOperator;
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
