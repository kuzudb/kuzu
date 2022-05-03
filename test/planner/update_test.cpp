#include "test/planner/planner_test_helper.h"

class UpdateTest : public PlannerTest {};

TEST_F(UpdateTest, BasicSetTest) {
    auto query = "MATCH (a:person) SET a.age = 35, a.name = 'Alice'; ";
    // SCAN(a.ID) -> SET
    auto plan = getBestPlan(query);
    auto op1 = plan->getLastOperator();
    ASSERT_EQ(LOGICAL_SET, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0);
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op2->getLogicalOperatorType());
}

TEST_F(UpdateTest, TwoHopSetTest) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) SET a.age = b.age;";
    // SCAN(b.ID) -> SCAN(b.age) -> FLATTEN -> EXTEND (a.ID) -> SET
    auto plan = getBestPlan(query);
    auto s = plan->getLastOperator()->toString();
    auto op1 = plan->getLastOperator();
    ASSERT_EQ(LOGICAL_SET, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0);
    ASSERT_EQ(LOGICAL_EXTEND, op2->getLogicalOperatorType());
    auto op3 = op2->getChild(0)->getChild(0);
    ASSERT_EQ(LOGICAL_SCAN_NODE_PROPERTY, op3->getLogicalOperatorType());
}

TEST_F(UpdateTest, ReturnSetTest) {
    auto query = "MATCH (a:person) SET a.age = 35 SET a.age = 40 RETURN a.name;";
    // SCAN(a.ID) -> SCAN(a.name) -> SET -> SET
    auto plan = getBestPlan(query);
    auto op1 = plan->getLastOperator()->getChild(0);
    ASSERT_EQ(LOGICAL_PROJECTION, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0);
    ASSERT_EQ(LOGICAL_SET, op2->getLogicalOperatorType());
    auto op3 = op2->getChild(0);
    ASSERT_EQ(LOGICAL_SET, op3->getLogicalOperatorType());
}

TEST_F(UpdateTest, MultiPartSetTest) {
    auto query =
        "MATCH (a:person) SET a.age = 35 WITH a MATCH (a)-[:knows]->(b:person) SET b.age= 40 ;";
    // SCAN(a.ID) -> SET -> PROJECTION -> FLATTEN -> EXTEND (b.ID) -> SET
    auto plan = getBestPlan(query);
    auto s = plan->getLastOperator()->toString();
    auto op1 = plan->getLastOperator();
    ASSERT_EQ(LOGICAL_SET, op1->getLogicalOperatorType());
    auto op2 = op1->getChild(0)->getChild(0)->getChild(0)->getChild(0);
    ASSERT_EQ(LOGICAL_SET, op2->getLogicalOperatorType());
}
