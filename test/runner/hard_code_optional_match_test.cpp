#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

#include "src/binder/include/expression/node_expression.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/nested_loop_join/logical_left_nested_loop_join.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/select_scan/logical_result_scan.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"

using ::testing::Test;
using namespace graphflow::testing;

class TinySnbHardCodeOptionalMatchTest : public DBLoadedTest {

public:
    void SetUp() override {
        DBLoadedTest::SetUp();
        profiler = make_unique<Profiler>();
        memoryManager = make_unique<MemoryManager>();
        context = make_unique<ExecutionContext>(*profiler, memoryManager.get());
    }

    unique_ptr<NodeExpression> createNodeExpression(const string& name, label_t label) {
        return make_unique<NodeExpression>(name, label);
    }

    unique_ptr<Schema> createListExtendSchema(const vector<string>& vars) {
        auto schema = make_unique<Schema>();
        for (auto& var : vars) {
            schema->insertToGroup(var, schema->createGroup());
        }
        return schema;
    }

    unique_ptr<Schema> createColExtendSchema(const vector<string>& vars) {
        auto schema = make_unique<Schema>();
        auto pos = schema->createGroup();
        for (auto& var : vars) {
            schema->insertToGroup(var, pos);
        }
        return schema;
    }

    shared_ptr<LogicalOperator> createSingleNodeScanAndFlatten(const string& var, label_t label) {
        auto logicalScan = make_shared<LogicalScanNodeID>(var, label);
        return make_shared<LogicalFlatten>(var, logicalScan);
    }

    unique_ptr<LogicalPlan> createLogicalPlan(
        shared_ptr<LogicalOperator> op, unique_ptr<Schema> schema) {
        auto plan = make_unique<LogicalPlan>(move(schema));
        plan->appendOperator(move(op));
        return plan;
    }

    vector<string> mapAndExecuteLogicalPlan(unique_ptr<LogicalPlan> logicalPlan) {
        auto mapper = PlanMapper(*defaultSystem->graph);
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan), *context);
        auto& resultCollector = (ResultCollector&)*physicalPlan->lastOperator;
        resultCollector.init();
        resultCollector.execute();
        return TestHelper::getActualOutput(*resultCollector.queryResult);
    }

    string getInputCSVDir() override { return "dataset/tinysnb/"; }

public:
    unique_ptr<Profiler> profiler;
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<ExecutionContext> context;
};

/**
 *      MATCH (a:person)
 *      OPTIONAL MATCH (a)-[:knows]->(:Person)
 *      RETURN COUNT(*)
 * Plan: SCAN(a)->Flatten(a)->LeftNestedLoopJoin(subPlan)->ResultCollector
 * SubPlan: ResultScan()->ListExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodeOptionalMatchTest, Test1) {
    auto nodeIDsToProject = vector<shared_ptr<Expression>>{
        createNodeExpression("a.id", 0), createNodeExpression("b.id", 0)};
    auto outerFlatten = createSingleNodeScanAndFlatten("a.id", 0);
    // start inner plan
    auto innerResultScan = make_shared<LogicalResultScan>(unordered_set<string>{"a.id"});
    auto innerExtend = make_shared<LogicalExtend>(
        "a.id", 0, "b.id", 0, 0, FWD, false, 1, 1, move(innerResultScan));
    auto innerProjection =
        make_shared<LogicalProjection>(nodeIDsToProject, vector<uint32_t>{}, move(innerExtend));
    auto subPlan = createLogicalPlan(
        move(innerProjection), createListExtendSchema(vector<string>{"a.id", "b.id"}));
    // end inner plan
    auto outerLeftNLJ = make_shared<LogicalLeftNestedLoopJoin>(move(subPlan), outerFlatten);
    auto outerProjection =
        make_shared<LogicalProjection>(nodeIDsToProject, vector<uint32_t>{}, move(outerLeftNLJ));
    auto logicalPlan = createLogicalPlan(
        move(outerProjection), createListExtendSchema(vector<string>{"a.id", "b.id"}));
    logicalPlan->schema->expressionsToCollect = nodeIDsToProject;
    auto result = mapAndExecuteLogicalPlan(move(logicalPlan));
    ASSERT_TRUE(result.size() == 17);
}

/**
 *      MATCH (a:person)
 *      OPTIONAL MATCH (a)-[:knows]->(b:Person)
 *      OPTIONAL MATCH (b)-[:knows]->(c:Person)
 *      RETURN COUNT(*)
 * Plan: SCAN(a)->Flatten(a)->LeftNestedLoopJoin(subPlan)->ResultCollector
 * SubPlan1: ResultScan(a)->ListExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodeOptionalMatchTest, Test2) {
    auto nodeIDsToProject = vector<shared_ptr<Expression>>{createNodeExpression("c.id", 0)};
    auto outerFlatten = createSingleNodeScanAndFlatten("a.id", 0);
    // start inner plan
    auto innerResultScan = make_shared<LogicalResultScan>(unordered_set<string>{"a.id"});
    auto innerExtend = make_shared<LogicalExtend>(
        "a.id", 0, "b.id", 0, 0, FWD, false, 1, 1, move(innerResultScan));
    auto innerFlatten = make_shared<LogicalFlatten>("b.id", move(innerExtend));
    // start inner inner plan
    auto innerInnerResultScan = make_shared<LogicalResultScan>(unordered_set<string>{"b.id"});
    auto innerInnerExtend = make_shared<LogicalExtend>(
        "b.id", 0, "c.id", 0, 0, FWD, false, 1, 1, move(innerInnerResultScan));
    auto innerInnerProjection = make_shared<LogicalProjection>(
        nodeIDsToProject, vector<uint32_t>{}, move(innerInnerExtend));
    auto subSubPlan = createLogicalPlan(
        move(innerInnerProjection), createListExtendSchema(vector<string>{"b.id", "c.id"}));
    // end inner inner plan
    auto innerLeftNLJ =
        make_shared<LogicalLeftNestedLoopJoin>(move(subSubPlan), move(innerFlatten));
    auto innerProjection =
        make_shared<LogicalProjection>(nodeIDsToProject, vector<uint32_t>{}, move(innerLeftNLJ));
    auto subPlan = createLogicalPlan(
        move(innerProjection), createListExtendSchema(vector<string>{"a.id", "b.id", "c.id"}));
    // end inner plan
    auto outerLeftNLJ = make_shared<LogicalLeftNestedLoopJoin>(move(subPlan), move(outerFlatten));
    auto outerProjection =
        make_shared<LogicalProjection>(nodeIDsToProject, vector<uint32_t>{}, move(outerLeftNLJ));
    auto logicalPlan = createLogicalPlan(
        move(outerProjection), createListExtendSchema(vector<string>{"a.id", "b.id", "c.id"}));
    logicalPlan->schema->expressionsToCollect = nodeIDsToProject;
    auto result = mapAndExecuteLogicalPlan(move(logicalPlan));
    ASSERT_TRUE(result.size() == 41);
}

/**
 *      MATCH (a:person)
 *      OPTIONAL MATCH (a)-[:studyAt]->(b:Organisation)
 *      RETURN *
 * Plan: SCAN(a)->Flatten(a)->LeftNestedLoopJoin(subPlan)->ResultCollector
 * SubPlan: ResultScan()->ColExtend(b)->ResultCollector
 *
 * Note that (a)-[:studyAt]->(b:Organisation) is a column extend. So the inner resultSet contains 1
 * dataChunk with 2 vectors. However, when we refer (shallow copy) the vector b from inner query to
 * outer query we copy it into a different dataChunk from a. This is because the inner dataChunk a
 * and outer dataChunk a has different state (inner state always have size = 1), The outer resultSet
 * contains 2 dataChunks each has 1 value vector for a and b respectively.
 */
TEST_F(TinySnbHardCodeOptionalMatchTest, Test3) {
    auto nodeIDsToProject = vector<shared_ptr<Expression>>{
        createNodeExpression("a.id", 0), createNodeExpression("b.id", 1)};
    auto outerFlatten = createSingleNodeScanAndFlatten("a.id", 0);
    // start inner plan
    auto innerResultScan = make_shared<LogicalResultScan>(unordered_set<string>{"a.id"});
    auto innerExtend =
        make_shared<LogicalExtend>("a.id", 0, "b.id", 1, 1, FWD, true, 1, 1, move(innerResultScan));
    auto innerProjection =
        make_shared<LogicalProjection>(nodeIDsToProject, vector<uint32_t>{}, move(innerExtend));
    auto subPlan = createLogicalPlan(
        move(innerProjection), createColExtendSchema(vector<string>{"a.id", "b.id"}));
    // end inner plan
    auto outerLeftNLJ = make_shared<LogicalLeftNestedLoopJoin>(move(subPlan), outerFlatten);
    auto outerProjection =
        make_shared<LogicalProjection>(nodeIDsToProject, vector<uint32_t>{}, move(outerLeftNLJ));
    auto logicalPlan = createLogicalPlan(
        move(outerProjection), createListExtendSchema(vector<string>{"a.id", "b.id"}));
    logicalPlan->schema->expressionsToCollect = nodeIDsToProject;
    auto result = mapAndExecuteLogicalPlan(move(logicalPlan));
    ASSERT_TRUE(result.size() == 8);
}
