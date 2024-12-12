#include "graph_test/graph_test.h"
#include "planner/operator/logical_plan_util.h"
#include "test_runner/test_runner.h"

namespace kuzu {
namespace testing {

class CardinalityTest : public DBTest {
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

TEST_F(CardinalityTest, TestOperators) {
    // Filter
    {
        // we only get cardinalities of operators created by the optimizer if we use EXPLAIN LOGICAL
        auto plan = getRoot("EXPLAIN LOGICAL MATCH (p1: person) WHERE p1.gender=1 RETURN p1.ID");
        auto [parent, source] = getSource(plan->getLastOperator().get());
        EXPECT_EQ(planner::LogicalOperatorType::SCAN_NODE_TABLE, source->getOperatorType());
        EXPECT_EQ(8, source->getCardinality());
        EXPECT_EQ(planner::LogicalOperatorType::FILTER, parent->getOperatorType());
        EXPECT_EQ(4, parent->getCardinality());
    }

    // Limit
    {
        auto plan = getRoot("EXPLAIN LOGICAL MATCH (p1: person) RETURN p1.ID LIMIT 2");
        auto* limitOp =
            getOpWithType(plan->getLastOperator().get(), planner::LogicalOperatorType::LIMIT);
        ASSERT_NE(nullptr, limitOp);
        EXPECT_EQ(planner::LogicalOperatorType::LIMIT, limitOp->getOperatorType());
        EXPECT_EQ(2, limitOp->getCardinality());
    }

    // Aggregate
    {
        auto plan = getRoot("EXPLAIN LOGICAL MATCH (p1: person) RETURN COUNT(*), MAX(p1.age)");
        auto* aggregateOp =
            getOpWithType(plan->getLastOperator().get(), planner::LogicalOperatorType::AGGREGATE);
        ASSERT_NE(nullptr, aggregateOp);
        EXPECT_EQ(1, aggregateOp->getCardinality());
    }

    // Cross Product
    {
        auto plan = getRoot("EXPLAIN LOGICAL MATCH (p1: person), (p2: person) RETURN p1.ID, p2.ID");
        auto* productOp = getOpWithType(plan->getLastOperator().get(),
            planner::LogicalOperatorType::CROSS_PRODUCT);
        ASSERT_NE(nullptr, productOp);
        EXPECT_EQ(64, productOp->getCardinality());
    }

    // Hash Join (non-ID based join)
    {
        auto plan = getRoot("EXPLAIN LOGICAL MATCH (p: person), (o: organisation) WHERE p.ID = "
                            "o.ID RETURN p.fName, o.name");
        auto* joinOp =
            getOpWithType(plan->getLastOperator().get(), planner::LogicalOperatorType::HASH_JOIN);
        ASSERT_NE(nullptr, joinOp);
        EXPECT_EQ(1, joinOp->getCardinality());
    }

    // Extend + Hash Join
    {
        auto plan = getRoot(
            "EXPLAIN LOGICAL MATCH (p1: person)-[k:knows]->(p2: person) RETURN p1.ID, p2.ID");
        auto* extendOp =
            getOpWithType(plan->getLastOperator().get(), planner::LogicalOperatorType::EXTEND);
        ASSERT_NE(nullptr, extendOp);
        static constexpr auto numRelsInKnows = 14;
        EXPECT_EQ(numRelsInKnows, extendOp->getCardinality());

        auto* joinOp =
            getOpWithType(plan->getLastOperator().get(), planner::LogicalOperatorType::HASH_JOIN);
        ASSERT_NE(nullptr, joinOp);
        EXPECT_GT(joinOp->getCardinality(), 1);
    }

    // Intersect + Flatten
    {
        auto plan = getRoot(
            "EXPLAIN LOGICAL MATCH (p1: person)-[k1:knows]->(p2: person)-[k2:knows]->(p3:person), "
            "(p1)-[k3:knows]->(p3) "
            "HINT ((p1 JOIN k1 JOIN p2) MULTI_JOIN k2 MULTI_JOIN k3) JOIN p3 "
            "RETURN p1.ID, p2.ID, p3.ID");
        auto* intersect =
            getOpWithType(plan->getLastOperator().get(), planner::LogicalOperatorType::INTERSECT);
        ASSERT_NE(nullptr, intersect);
        EXPECT_EQ(intersect->getCardinality(), 1);

        auto* flatten =
            getOpWithType(plan->getLastOperator().get(), planner::LogicalOperatorType::FLATTEN);
        ASSERT_NE(nullptr, intersect);
        EXPECT_GT(flatten->getCardinality(), 1);
    }

    // Load From Parquet
    {
        auto plan = getRoot(common::stringFormat(
            "LOAD FROM \"{}/dataset/demo-db/parquet/user.parquet\" RETURN *", KUZU_ROOT_DIRECTORY));
        EXPECT_EQ(4, plan->getCardinality());
    }

    // Load From Numpy
    {
        auto plan = getRoot(common::stringFormat(
            "LOAD FROM \"{}/dataset/npy-1d/one_dim_int64.npy\" RETURN *", KUZU_ROOT_DIRECTORY));
        EXPECT_EQ(3, plan->getCardinality());
    }
}

TEST_F(CardinalityTest, TestPopulatedAfterOptimizations) {
    // Filter push down
    auto plan = getRoot("EXPLAIN LOGICAL MATCH (a:person)-[e]->(b) "
                        "WHERE a.ID < 0 AND a.fName='Alice' "
                        "RETURN a.gender;");
    std::function<void(planner::LogicalOperator*)> checkFunc;
    checkFunc = [&checkFunc](planner::LogicalOperator* op) {
        EXPECT_GT(op->getCardinality(), 0);
        for (uint32_t i = 0; i < op->getNumChildren(); ++i) {
            checkFunc(op->getChild(i).get());
        }
    };
    checkFunc(plan->getLastOperator().get());
}

} // namespace testing
} // namespace kuzu
