#include "test/planner/planner_test_helper.h"

#include "src/planner/include/logical_plan/operator/nested_loop_join/logical_left_nested_loop_join.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"

class PropertyScanPushDownTest : public PlannerTest {};

// Assume optimizer picks QVO: b, a
TEST_F(PropertyScanPushDownTest, FilterPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = b.age RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto op = plan->lastOperator->getChild(0)
                  ->getChild(0)
                  ->getChild(0)
                  ->getChild(0)
                  ->getChild(0)
                  ->getChild(0)
                  ->getChild(0)
                  ->getChild(0)
                  .get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op->getLogicalOperatorType());
    auto scanNodeProperty = (LogicalScanNodeProperty*)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty->nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

// Assume optimizer picks QVO: b, a
TEST_F(PropertyScanPushDownTest, ProjectionPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) RETURN a.age, b.age";
    auto plan = getBestPlan(query);
    auto op =
        plan->lastOperator->getChild(0)->getChild(0)->getChild(0)->getChild(0)->getChild(0).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op->getLogicalOperatorType());
    auto scanNodeProperty = (LogicalScanNodeProperty*)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty->nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

// This test is to capture the bug where operator is not cloned (might lead to a bug where change of
// prevOperator of a plan affects other plan) before property push down optimization
TEST_F(PropertyScanPushDownTest, LogicalPlanCloneTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person)-[:knows]->(d:person) "
                 "Return b.age";
    // if logical plan is not cloned before optimization, plan1 will have repeated scanNodeProperty
    auto plan = move(getAllPlans(query)[1]);
    auto op = plan->lastOperator->getChild(0).get();
    while (op->getLogicalOperatorType() != LOGICAL_SCAN_NODE_PROPERTY) {
        op = op->getChild(0).get();
    }
    ASSERT_TRUE(op->getChild(0)->getLogicalOperatorType() != LOGICAL_SCAN_NODE_PROPERTY);
}

TEST_F(PropertyScanPushDownTest, SubPlanPropertyPushDownTest) {
    auto query = "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) RETURN a.age, b.age";
    auto plan = getBestPlan(query);
    auto leftNLJ = (LogicalLeftNestedLoopJoin*)plan->lastOperator->getChild(0)->getChild(0).get();
    auto op = leftNLJ->getChild(1).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op->getLogicalOperatorType());
    auto scanNodeProperty = (LogicalScanNodeProperty*)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty->nodeID, "_b." + INTERNAL_ID_SUFFIX));
}
