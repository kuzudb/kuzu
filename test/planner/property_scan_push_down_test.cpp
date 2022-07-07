#include "test/planner/planner_test_helper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_left_nested_loop_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_property.h"

class PropertyScanPushDownTest : public PlannerTest {};

// Assume optimizer picks QVO: a, b
TEST_F(PropertyScanPushDownTest, FilterPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = b.age RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto op = plan->getLastOperator()
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
    ASSERT_TRUE(containSubstr(scanNodeProperty->getNodeID(), "_a." + INTERNAL_ID_SUFFIX));
}

// Assume optimizer picks QVO: b, a
TEST_F(PropertyScanPushDownTest, ProjectionPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) RETURN a.age, b.age";
    auto plan = getBestPlan(query);
    auto op = plan->getLastOperator()->getChild(0)->getChild(0)->getChild(0)->getChild(0).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op->getLogicalOperatorType());
    auto scanNodeProperty = (LogicalScanNodeProperty*)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty->getNodeID(), "_a." + INTERNAL_ID_SUFFIX));
}

TEST_F(PropertyScanPushDownTest, SubPlanPropertyPushDownTest) {
    auto query = "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) RETURN a.age, b.age";
    auto plan = getBestPlan(query);
    auto leftNLJ = (LogicalLeftNestedLoopJoin*)plan->getLastOperator()->getChild(0).get();
    auto op = leftNLJ->getChild(1).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op->getLogicalOperatorType());
    auto scanNodeProperty = (LogicalScanNodeProperty*)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty->getNodeID(), "_b." + INTERNAL_ID_SUFFIX));
}
