#include "test/planner/planner_test_helper.h"

class CostModelTest : public PlannerTest {};

TEST_F(CostModelTest, OneHop) {
    auto query = "MATCH (a:person)-[:knows]->(b:person) RETURN MIN(a.age)";
    auto plan = getBestPlan(query);
    ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), "HJ(b){E(b)S(a)}{S(b)}");
}