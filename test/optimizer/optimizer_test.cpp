#include "graph_test/graph_test.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_plan_util.h"
#include "test_runner/test_runner.h"

namespace kuzu {
namespace testing {

class OptimizerTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    std::string getEncodedPlan(const std::string& query) {
        return planner::LogicalPlanUtil::encodeJoin(*TestRunner::getLogicalPlan(query, *conn));
    }
    std::unique_ptr<planner::LogicalPlan> getRoot(const std::string& query) {
        return TestRunner::getLogicalPlan(query, *conn);
    }
};

TEST_F(OptimizerTest, JoinHint) {
    // One hop
    auto q1 = "MATCH (a:person)-[e:knows]->(b:person) "
              "WHERE a.ID > 0 "
              "  AND e.date = date('1999-01-01') "
              "  AND b.ID > 2"
              "  AND a.ID + b.ID > 3 "
              "HINT a JOIN (e JOIN b) "
              "RETURN *";
    ASSERT_STREQ(getEncodedPlan(q1).c_str(),
        "Filter()HJ(a._ID){Filter()S(a)}{Filter()E(a)Filter()S(b)}");
    auto q2 = "MATCH (a:person)-[e:knows]->(b:person) "
              "WHERE a.ID > 0 "
              "  AND e.date = date('1999-01-01') "
              "  AND b.ID > 2"
              "  AND a.ID + b.ID > 3 "
              "HINT (a JOIN e) JOIN b "
              "RETURN *";
    ASSERT_STREQ(getEncodedPlan(q2).c_str(),
        "Filter()HJ(b._ID){Filter()E(b)Filter()S(a)}{Filter()S(b)}");
    // Two hop
    auto q3 = "MATCH (a:person)-[e1:knows]->(b:person)-[e2:knows]->(c:person) "
              "HINT (((b JOIN e1) JOIN e2) JOIN a) JOIN c "
              "RETURN *";
    ASSERT_STREQ(getEncodedPlan(q3).c_str(), "HJ(c._ID){HJ(a._ID){E(c)E(a)S(b)}{S(a)}}{S(c)}");
    auto q4 = "MATCH (a:person)-[e1:knows]->(b:person)-[e2:knows]->(c:person) "
              "HINT (((a JOIN e1) JOIN b) JOIN e2) JOIN c "
              "RETURN COUNT(*)";
    ASSERT_STREQ(getEncodedPlan(q4).c_str(), "HJ(b._ID){E(b)S(a)}{E(c)S(b)}");
    // Cycle
    auto q5 = "MATCH (a:person)-[e1:knows]->(b:person)-[e2:knows]->(a) "
              "HINT ((a JOIN e1) JOIN b) JOIN e2 "
              "RETURN *";
    ASSERT_STREQ(getEncodedPlan(q5).c_str(),
        "HJ(a._ID,b._ID){HJ(b._ID){E(b)S(a)}{S(b)}}{E(a)S(b)}");
    auto q6 = "MATCH (a:person)-[e1:knows]->(b:person)-[e2:knows]->(c:person), "
              "      (a)-[e3:knows]->(c) "
              "HINT (((a JOIN e1) JOIN b) MULTI_JOIN e2 MULTI_JOIN e3) JOIN c "
              "RETURN *";
    ASSERT_STREQ(getEncodedPlan(q6).c_str(),
        "HJ(c._ID){I(c._ID){HJ(b._ID){E(b)S(a)}{S(b)}}{E(c)S(b)}{E(c)S(a)}}{S(c)}");
}

TEST_F(OptimizerTest, CrossJoinWithFilterWithoutPushDownTest) {
    auto q1 = "MATCH (a:person) "
              "MATCH (b:person) "
              "WHERE a.fName=b.fName AND a.gender <> b.gender "
              "RETURN a.gender;";
    ASSERT_STREQ(getEncodedPlan(q1).c_str(), "Filter()HJ(a.fName=b.fName){S(a)}{S(b)}");
    auto q2 = "MATCH (a:person) "
              "MATCH (b:person) "
              "WHERE a.fName=b.fName AND a.fName is NOT null "
              "RETURN a.fName;";
    ASSERT_STREQ(getEncodedPlan(q2).c_str(), "HJ(a.fName=b.fName){Filter()S(a)}{S(b)}");
    auto q3 = "MATCH (a:person) "
              "MATCH (b:person) "
              "WHERE a.fName=b.fName AND a.age > 1 + 2.0 "
              "RETURN a.fName;";
    ASSERT_STREQ(getEncodedPlan(q3).c_str(), "HJ(a.fName=b.fName){Filter()S(a)}{S(b)}");
}

TEST_F(OptimizerTest, DisableOptimizerTest) {
    conn->query("CALL enable_plan_optimizer=false");

    auto q1 = "MATCH (a:person)-[e]->(b) "
              "WHERE a.ID < 0 AND a.fName='Alice' "
              "RETURN a.gender;";
    {
        ASSERT_STREQ(getEncodedPlan(q1).c_str(), "HJ(b._ID){E(b)Filter()Filter()S(a)}{S(b)}");
        // sanity check to see if the plan still outputs the correct result
        auto result = conn->query(q1);
        ASSERT_EQ(result->getNumTuples(), 0);
    }

    auto q2 = "MATCH (a:person)-[e:knows]->(b:person) "
              "HINT (a JOIN e) JOIN b "
              "RETURN e.date;";
    {
        ASSERT_STREQ(getEncodedPlan(q2).c_str(), "HJ(b._ID){E(b)S(a)}{S(b)}");
        auto result = conn->query(q2);
        ASSERT_EQ(result->getNumTuples(), 14);
    }

    conn->query("CALL enable_plan_optimizer=true");
    {
        ASSERT_STREQ(getEncodedPlan(q2).c_str(), "E(b)S(a)");
        auto result = conn->query(q2);
        ASSERT_EQ(result->getNumTuples(), 14);
    }
}

TEST_F(OptimizerTest, FilterPushDownTest) {
    auto q1 = "MATCH (a:person)-[e]->(b) "
              "WHERE a.ID < 0 AND a.fName='Alice' "
              "RETURN a.gender;";
    ASSERT_STREQ(getEncodedPlan(q1).c_str(), "E(b)Filter()Filter()S(a)");
}

TEST_F(OptimizerTest, IndexScanTest) {
    auto q1 = "MATCH (a:person) "
              "WHERE a.ID = 0 AND a.fName='Alice' "
              "RETURN a.gender;";
    ASSERT_STREQ(getEncodedPlan(q1).c_str(), "Filter()IndexScan(a)");
}

TEST_F(OptimizerTest, RemoveUnnecessaryJoinTest) {
    auto q1 = "MATCH (a:person)-[e:knows]->(b:person) "
              "HINT (a JOIN e) JOIN b "
              "RETURN e.date;";
    ASSERT_STREQ(getEncodedPlan(q1).c_str(), "E(b)S(a)");
}

TEST_F(OptimizerTest, PkScanTest) {
    auto q1 = "MATCH (a:person {ID:24189255811663})-[f]->(b) RETURN b;";
    auto ans = getEncodedPlan(q1);
    ASSERT_STREQ(ans.c_str(), "HJ(b._ID){E(b)IndexScan(a)}{S(b)}");
}

TEST_F(OptimizerTest, FilterDifferentPropertiesTest) {
    auto q1 = "MATCH (a:person {gender:1})-[f]->(b:person {age: 30}) RETURN b;";
    auto ans = getEncodedPlan(q1);
    ASSERT_STREQ(ans.c_str(), "HJ(a._ID){E(a)Filter()S(b)}{Filter()S(a)}");
}

TEST_F(OptimizerTest, SingleNodeTwoHopJoins) {
#if defined(WIN32)
    // Skip on windows as we don't generate consistent plan as on other platforms.
    // TODO(Guodong/Xiyang): We should make sure the plan is consistent on all platforms.
    GTEST_SKIP();
#else
    auto q1 =
        "MATCH (a:person)-[e:knows]->(b:person)-[e2:knows]->(c:person) WHERE b.ID=0 RETURN a,b,c;";
    ASSERT_STREQ(getEncodedPlan(q1).c_str(),
        "HJ(c._ID){S(c)}{HJ(a._ID){S(a)}{E(c)E(a)IndexScan(b)}}");
    auto q2 = "MATCH (a:person)-[e:knows]->(b:person)-[e2:knows]->(c:person) WHERE a.ID=0 "
              "RETURN a,b,c;";
    ASSERT_STREQ(getEncodedPlan(q2).c_str(),
        "HJ(c._ID){S(c)}{HJ(b._ID){E(c)S(b)}{E(b)IndexScan(a)}}");
    auto q3 = "MATCH (a:person)-[e:knows]->(b:person)-[e2:knows]->(c:person) WHERE c.ID=0 "
              "RETURN a,b,c;";
    ASSERT_STREQ(getEncodedPlan(q3).c_str(),
        "HJ(a._ID){S(a)}{HJ(b._ID){E(a)S(b)}{E(b)IndexScan(c)}}");
#endif
}

TEST_F(OptimizerTest, PlanUndirectedInnerJoin) {
    auto query = "MATCH (a:person)-[e:knows]-(b:person) RETURN a.ID, b.ID;";
    auto encodedPlan = getEncodedPlan(query);
    // there should only be a single hash join in the plan
    ASSERT_TRUE((encodedPlan == "HJ(b._ID){E(b)S(a)}{S(b)}") ||
                (encodedPlan == "HJ(a._ID){E(a)S(b)}{S(a)}"));
}

} // namespace testing
} // namespace kuzu
