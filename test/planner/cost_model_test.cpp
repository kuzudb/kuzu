#include "test/planner/planner_test_helper.h"

class CostModelTest : public PlannerTest {};

TEST_F(CostModelTest, OneHop) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) RETURN MIN(a.age)";
    auto plan = getBestPlan(query);
    ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), "HJ(b){E(b)S(a)}{S(b)}");
}

TEST_F(CostModelTest, TwoHops) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    ASSERT_STREQ(
        LogicalPlanUtil::encodeJoin(*plan).c_str(), "HJ(a){HJ(c){E(a)E(c)S(b)}{S(c)}}{S(a)}");
}

TEST_F(CostModelTest, ThreeStars) {
    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person), "
                 "(b)-[:knows]->(d:person) RETURN COUNT(*)";
    auto plan = getBestPlan(query);
    ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(),
        "HJ(a){HJ(c){HJ(d){E(a)E(d)E(c)S(b)}{S(d)}}{S(c)}}{S(a)}");
}
