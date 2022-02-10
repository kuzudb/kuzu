#include "test/planner/planner_test_helper.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"

class CostModelTest : public PlannerTest {

public:
};

TEST_F(CostModelTest, OneHopSingleFilter) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = 1 RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto op1 = plan->lastOperator->getChild(0)->getChild(0)->getChild(0)->getChild(0).get();
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
    auto op1 = plan->lastOperator->getChild(0)
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

TEST_F(CostModelTest, TwoHop) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto op1 = plan->lastOperator->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   .get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op1->getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID*)op1)->nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

TEST_F(CostModelTest, TwoHopMultiFilters) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) WHERE a.age = 20 AND "
                 "b.age = 35 RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    auto op1 = plan->lastOperator->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   ->getChild(0)
                   .get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op1->getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID*)op1)->nodeID, "_b." + INTERNAL_ID_SUFFIX));
}

// temporary tests for order by logical plans

TEST_F(PlannerTest, OrderByTest1) {
    auto query = "MATCH (a:person) RETURN a.age ORDER BY a.age";
    auto plan = getBestPlan(query);
    auto op1 = plan->lastOperator->getChild(0).get();
    ASSERT_EQ(LOGICAL_PROJECTION, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0).get();
    ASSERT_EQ(LOGICAL_ORDER_BY, op2->getLogicalOperatorType());
    auto op3 = op2->getChild(0).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op3->getLogicalOperatorType());
}

// best order a,b
TEST_F(PlannerTest, OrderByTest2) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) RETURN b.age ORDER BY a.age";
    auto plan = getBestPlan(query);
    auto op1 = plan->lastOperator->getChild(0).get();
    ASSERT_EQ(LOGICAL_PROJECTION, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0).get();
    ASSERT_EQ(LOGICAL_ORDER_BY, op2->getLogicalOperatorType());
    auto op3 = op2->getChild(0).get();
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op3->getLogicalOperatorType());
    auto op4 = op3->getChild(0).get();
    ASSERT_EQ(LOGICAL_EXTEND, op4->getLogicalOperatorType());
}

TEST_F(PlannerTest, OrderByTest3) {
    auto query = "MATCH (a:person) RETURN COUNT(*) ORDER BY COUNT(*)";
    auto plan = getBestPlan(query);
    auto op1 = plan->lastOperator->getChild(0)->getChild(0).get();
    ASSERT_EQ(LOGICAL_ORDER_BY, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0).get();
    ASSERT_EQ(LOGICAL_AGGREGATE, op2->getLogicalOperatorType());
}
