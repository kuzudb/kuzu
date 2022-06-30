#include "test/planner/planner_test_helper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"

class CostModelTest : public PlannerTest {};

TEST_F(CostModelTest, OneHopSingleFilter) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = 1 RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto op1 = plan->getLastOperator()->getChild(0)->getChild(0)->getChild(0).get();
    ASSERT_EQ(LOGICAL_EXTEND, op1->getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalExtend*)op1)->nbrNodeID, "_b." + INTERNAL_ID_SUFFIX));
    auto op2 = op1->getChild(0)->getChild(0)->getChild(0)->getChild(0).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op2->getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID*)op2)->nodeID, "_a." + INTERNAL_ID_SUFFIX));
}

TEST_F(CostModelTest, OneHopMultiFilters) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age > 10 AND a.age < 20 AND b.age "
                 "= 45 RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto op1 = plan->getLastOperator()
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   .get();
    ASSERT_EQ(LOGICAL_EXTEND, op1->getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalExtend*)op1)->nbrNodeID, "_b." + INTERNAL_ID_SUFFIX));
    auto op2 = op1->getChild(0)->getChild(0)->getChild(0)->getChild(0)->getChild(0).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op2->getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID*)op2)->nodeID, "_a." + INTERNAL_ID_SUFFIX));
}

TEST_F(PlannerTest, OrderByTest3) {
    auto query = "MATCH (a:person) RETURN COUNT(*) ORDER BY COUNT(*)";
    auto plan = getBestPlan(query);
    auto op1 = plan->getLastOperator()->getChild(0).get();
    ASSERT_EQ(LOGICAL_ORDER_BY, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0).get();
    ASSERT_EQ(LOGICAL_AGGREGATE, op2->getLogicalOperatorType());
}
