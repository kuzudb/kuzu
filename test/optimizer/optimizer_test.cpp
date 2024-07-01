#include "graph_test/graph_test.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_plan_util.h"
#include "planner/operator/scan/logical_scan_node_table.h"
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

TEST_F(OptimizerTest, RecursiveJoinTest) {
    auto q1 = "MATCH (a:person)-[e:knows* SHORTEST 1..5]->(b:person) "
              "WHERE b.ID < 0 "
              "RETURN e, a.fName;";
    ASSERT_STREQ(getEncodedPlan(q1).c_str(), "HJ(a._ID){RE(a)Filter()S(b)}{S(a)}");
    auto q2 = "MATCH (a:person)-[e:knows* SHORTEST 1..5]->(b:person) "
              "HINT (a JOIN e) JOIN b "
              "RETURN length(e);";
    ASSERT_STREQ(getEncodedPlan(q2).c_str(), "RE_NO_TRACK(b)S(a)");
}

} // namespace testing
} // namespace kuzu
