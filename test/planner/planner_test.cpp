#include "graph_test/graph_test.h"
#include "planner/operator/logical_plan_util.h"
#include "test_runner/test_runner.h"

namespace kuzu {
namespace testing {

class PlannerTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    std::string getEncodedPlan(const std::string& query) {
        return planner::LogicalPlanUtil::encodeJoin(*getRoot(query));
    }
    std::unique_ptr<planner::LogicalPlan> getRoot(const std::string& query) {
        return TestRunner::getLogicalPlan(query, *conn);
    }
    std::pair<planner::LogicalOperator*, planner::LogicalOperator*> getSource(
        planner::LogicalOperator* op, planner::LogicalOperator* parent = nullptr) {
        if (op->getNumChildren() == 0) {
            return {parent, op};
        }
        return getSource(op->getChild(0).get(), op);
    }
    planner::LogicalOperator* getOpWithType(planner::LogicalOperator* op,
        planner::LogicalOperatorType type) {
        if (op->getOperatorType() == type) {
            return op;
        }
        if (op->getNumChildren() == 0) {
            return nullptr;
        }
        return getOpWithType(op->getChild(0).get(), type);
    }
};

TEST_F(PlannerTest, TestSubqueryJoinOrder) {
    // Cross Product
    {
        // We should pick the smaller table to be on the build side
        auto query = "MATCH (a:person) WITH a MATCH (b:organisation) RETURN *";
        EXPECT_STREQ("CP(){S(a)}{S(b)}", getEncodedPlan(query).c_str());
        auto queryFlipped = "MATCH (b:organisation) WITH b MATCH (a:person) RETURN *";
        EXPECT_STREQ("CP(){S(a)}{S(b)}", getEncodedPlan(queryFlipped).c_str());
    }

    // Hash Join
    {
        // cardinality(person) > cardinality(studyAt)
        // scan(person) should go on probe side
        auto query = "MATCH (a:person) WITH a MATCH (a)-[s:studyAt]->(b:organisation) RETURN *";
        EXPECT_STREQ("HJ(a._ID){S(a)}{HJ(b._ID){S(b)}{E(b)S(a)}}", getEncodedPlan(query).c_str());

        // cardinality(organisation) < cardinality(studyAt) + cardinality(workAt)
        // scan(organisation) should go on build side
        auto queryFlipped =
            "MATCH (b:organisation) WITH b MATCH (a:person)-[s:studyAt|:workAt]->(b) RETURN *";
        EXPECT_STREQ("HJ(b._ID){E(b)S(a)}{S(b)}", getEncodedPlan(queryFlipped).c_str());
    }
}

} // namespace testing
} // namespace kuzu
