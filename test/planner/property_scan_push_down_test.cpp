#include "test/planner/planner_test_helper.h"

#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"

class PropertyScanPushDownTest : public PlannerTest {};

// Assume optimizer picks QVO: b, a
TEST_F(PropertyScanPushDownTest, FilterPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = b.age RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto& op =
        *plan->lastOperator->prevOperator->prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op.getLogicalOperatorType());
    auto& scanNodeProperty = (LogicalScanNodeProperty&)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty.nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

// Assume optimizer picks QVO: b, a
TEST_F(PropertyScanPushDownTest, ProjectionPropertyPushDownTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) RETURN a.age, b.age";
    auto plan = getBestPlan(query);
    auto& op = *plan->lastOperator->prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op.getLogicalOperatorType());
    auto& scanNodeProperty = (LogicalScanNodeProperty&)op;
    ASSERT_TRUE(containSubstr(scanNodeProperty.nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

// This test is to capture the bug where operator is not cloned (might lead to a bug where change of
// prevOperator of a plan affects other plan) before property push down optimization
TEST_F(PropertyScanPushDownTest, LogicalPlanCloneTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person)-[:knows]->(d:person) "
                 "Return b.age";
    // if logical plan is not cloned before optimization, plan1 will have repeated scanNodeProperty
    auto plan = move(getAllPlans(query)[1]);
    auto op = plan->lastOperator.get();
    while (op->getLogicalOperatorType() != LOGICAL_SCAN_NODE_PROPERTY) {
        op = op->prevOperator.get();
    }
    ASSERT_TRUE(op->prevOperator->getLogicalOperatorType() != LOGICAL_SCAN_NODE_PROPERTY);
}
