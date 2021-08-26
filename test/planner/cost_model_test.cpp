#include "test/planner/planner_test_helper.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"

class CostModelTest : public PlannerTest {};

TEST_F(CostModelTest, OneHopSingleFilter) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = 1 RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto& op1 = *plan->lastOperator;
    ASSERT_EQ(LOGICAL_EXTEND, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalExtend&)op1).nbrNodeID, "_b." + INTERNAL_ID_SUFFIX));
    auto& op2 = *op1.prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op2.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op2).nodeID, "_a." + INTERNAL_ID_SUFFIX));
}

TEST_F(CostModelTest, OneHopMultiFilters) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age > 10 AND a.age < 20 AND b.age "
                 "= 45 RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto& op1 = *plan->lastOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_EXTEND, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalExtend&)op1).nbrNodeID, "_b." + INTERNAL_ID_SUFFIX));
    auto& op2 = *op1.prevOperator->prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op2.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op2).nodeID, "_a." + INTERNAL_ID_SUFFIX));
}

TEST_F(CostModelTest, TwoHop) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto& op1 = *plan->lastOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op1).nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

TEST_F(CostModelTest, TwoHopMultiFilters) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) WHERE a.age = 20 AND "
                 "b.age = 35 RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto& op1 = *plan->lastOperator->prevOperator->prevOperator->prevOperator->prevOperator
                     ->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op1).nodeID, "_b." + INTERNAL_ID_SUFFIX));
}
