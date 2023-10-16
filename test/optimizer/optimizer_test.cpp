#include "graph_test/graph_test.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_plan_util.h"
#include "planner/operator/scan/logical_scan_node_property.h"
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

    std::shared_ptr<planner::LogicalOperator> getRoot(const std::string& query) {
        return TestRunner::getLogicalPlan(query, *conn)->getLastOperator();
    }
};

TEST_F(OptimizerTest, WithClauseProjectionListRewriterTest) {
    auto op = getRoot("MATCH (a:person) WITH a RETURN a.gender;");
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::PROJECTION);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::PROJECTION);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_NODE_PROPERTY);
    auto scanNodeProperty = (planner::LogicalScanNodeProperty*)op.get();
    ASSERT_EQ(scanNodeProperty->getProperties().size(), 1);
}

TEST_F(OptimizerTest, FilterPushDownTest) {
    auto op = getRoot("MATCH (a:person) WHERE a.ID < 0 AND a.fName='Alice' RETURN a.gender;");
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::PROJECTION);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_NODE_PROPERTY);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::FILTER);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_NODE_PROPERTY);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::FILTER);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_NODE_PROPERTY);
}

TEST_F(OptimizerTest, IndexScanTest) {
    auto op = getRoot("MATCH (a:person) WHERE a.ID = 0 AND a.fName='Alice' RETURN a.gender;");
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::PROJECTION);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_NODE_PROPERTY);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::FILTER);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_NODE_PROPERTY);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::INDEX_SCAN_NODE);
}

TEST_F(OptimizerTest, RemoveUnnecessaryJoinTest) {
    auto op = getRoot("MATCH (a:person)-[e:knows]->(b:person) RETURN e.date;");
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::PROJECTION);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::EXTEND);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::FLATTEN);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_INTERNAL_ID);
}

TEST_F(OptimizerTest, ProjectionPushDownJoinTest) {
    auto op = getRoot(
        "MATCH (a:person)-[e:knows]->(b:person) WHERE a.age > 0 AND b.age>0 RETURN a.ID, b.ID;");
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::PROJECTION);
    op = op->getChild(0);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::HASH_JOIN);
    op = op->getChild(1);
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::PROJECTION);
}

TEST_F(OptimizerTest, RecursiveJoinTest) {
    auto encodedPlan = getEncodedPlan(
        "MATCH (a:person)-[:knows* SHORTEST 1..5]->(b:person) WHERE b.ID < 0 RETURN a.fName;");
    ASSERT_STREQ(encodedPlan.c_str(), "HJ(a._ID){RE(a)S(b._ID)}{S(a._ID)}");
}

TEST_F(OptimizerTest, RecursiveJoinNoTrackPathTest) {
    auto op = getRoot("MATCH (a:person)-[e:knows* SHORTEST 1..3]->(b:person) RETURN length(e);");
    while (op->getOperatorType() != planner::LogicalOperatorType::RECURSIVE_EXTEND) {
        op = op->getChild(0);
    }
    auto recursiveExtend = (planner::LogicalRecursiveExtend*)op.get();
    ASSERT_TRUE(recursiveExtend->getJoinType() == planner::RecursiveJoinType::TRACK_NONE);
}

} // namespace testing
} // namespace kuzu
