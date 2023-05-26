#include "graph_test/graph_test.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "planner/logical_plan/logical_plan_util.h"

namespace kuzu {
namespace testing {

class OptimizerTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    std::string getEncodedPlan(const std::string& query) {
        return planner::LogicalPlanUtil::encodeJoin(*TestHelper::getLogicalPlan(query, *conn));
    }

    std::shared_ptr<planner::LogicalOperator> getRoot(const std::string& query) {
        return TestHelper::getLogicalPlan(query, *conn)->getLastOperator();
    }
};

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
    ASSERT_EQ(op->getOperatorType(), planner::LogicalOperatorType::SCAN_NODE);
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
    ASSERT_STREQ(encodedPlan.c_str(), "HJ(a._id){RE(a)S(b)}{S(a)}");
}

TEST_F(OptimizerTest, RecursiveJoinNoTrackPathTest) {
    auto op = getRoot("MATCH (a:person)-[e:knows* SHORTEST 2..3]->(b:person) RETURN length(e);");
    while (op->getOperatorType() != planner::LogicalOperatorType::RECURSIVE_EXTEND) {
        op = op->getChild(0);
    }
    auto recursiveExtend = (planner::LogicalRecursiveExtend*)op.get();
    ASSERT_TRUE(recursiveExtend->getJoinType() == planner::RecursiveJoinType::TRACK_NONE);
}

} // namespace testing
} // namespace kuzu
