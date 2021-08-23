#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/expression_evaluator/include/binary_expression_evaluator.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/select_scan/logical_select_scan.h"
#include "src/processor/include/physical_plan/operator/flatten/flatten.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/physical_plan/plan_mapper.h"

using ::testing::Test;
using namespace graphflow::testing;
using namespace graphflow::binder;

class TinySnbHardCodePlanTest : public DBLoadedTest {
public:
    void SetUp() override {
        DBLoadedTest::SetUp();
        profiler = make_unique<Profiler>();
        memoryManager = make_unique<MemoryManager>();
        context =
            make_unique<ExecutionContext>(*profiler, nullptr /*transaction*/, memoryManager.get());
    }

    unique_ptr<Schema> createANodeSchema() {
        auto schema = make_unique<Schema>();
        schema->appendToGroup("a._id", schema->createGroup());
        return schema;
    }

    unique_ptr<Schema> createListExtendSchema(const string& var1, const string& var2) {
        auto schema = make_unique<Schema>();
        schema->appendToGroup(var1, schema->createGroup());
        schema->appendToGroup(var2, schema->createGroup());
        return schema;
    }

    unique_ptr<Schema> createColExtendSchema(const string& var1, const string& var2) {
        auto schema = make_unique<Schema>();
        auto pos = schema->createGroup();
        schema->appendToGroup(var1, pos);
        schema->appendToGroup(var2, pos);
        return schema;
    }

    shared_ptr<LogicalOperator> createBasicScanAndFlattenA() {
        auto logicalScan = make_shared<LogicalScanNodeID>("a._id", 0 /* label */);
        return make_shared<LogicalFlatten>("a._id", logicalScan);
    }

    unique_ptr<LogicalPlan> createLogicalPlan(
        shared_ptr<LogicalOperator> op, unique_ptr<Schema> schema) {
        auto plan = make_unique<LogicalPlan>(move(schema));
        plan->appendOperator(move(op));
        return plan;
    }

    uint64_t mapAndExecuteLogicalPlan(unique_ptr<LogicalPlan> logicalPlan) {
        auto mapper = PlanMapper(*defaultSystem->graph);
        auto physicalPlan = mapper.mapToPhysical(move(logicalPlan), *context);
        auto& resultCollector = (ResultCollector&)*physicalPlan->lastOperator;
        resultCollector.execute();
        return resultCollector.queryResult->numTuples;
    }

    string getInputCSVDir() override { return "dataset/tinysnb/"; }

public:
    unique_ptr<Profiler> profiler;
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<ExecutionContext> context;
};

/**
 * MATCH (a:person) WHERE EXISTS {
 *          MATCH (a)-[:knows]->(:Person)
 *          RETURN *
 *      }
 * RETURN COUNT(*)
 * Plan: SCAN(a)->Flatten(a)->Filter(subPlan)->ResultCollector
 * SubPlan: SelectScan()->ListExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, LogicalSubqueryExistListTest) {
    auto schema = createANodeSchema();
    auto logicalFlatten = createBasicScanAndFlattenA();
    // start inner plan
    auto logicalSelectScan =
        make_shared<LogicalSelectScan>(vector<string>{"a._id"}, schema->copy());
    auto logicalExtend =
        make_shared<LogicalExtend>("a._id", 0, "b._id", 0, 0, FWD, false, 1, 1, logicalSelectScan);
    auto logicalSubPlan =
        createLogicalPlan(logicalExtend, createListExtendSchema("a._id", "b._id"));
    // end inner plan
    auto subqueryExpr = make_shared<ExistentialSubqueryExpression>(move(logicalSubPlan));
    auto logicalFilter = make_shared<LogicalFilter>(move(subqueryExpr), 0, logicalFlatten);
    auto logicalPlan = createLogicalPlan(logicalFilter, move(schema));

    ASSERT_TRUE(mapAndExecuteLogicalPlan(move(logicalPlan)) == 5);
}

/**
 * MATCH (a:person) WHERE NOT EXISTS {
 *          MATCH (a)-[:knows]->(b:Person)
 *          RETURN *
 *      }
 * RETURN COUNT(*)
 * Plan: SCAN(a)->Flatten(a)->Filter(subPlan)->ResultCollector
 * SubPlan: SelectScan()->ListExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, LogicalSubqueryNotExistListTest) {
    auto schema = createANodeSchema();
    auto logicalFlatten = createBasicScanAndFlattenA();
    // start inner plan
    auto logicalSelectScan =
        make_shared<LogicalSelectScan>(vector<string>{"a._id"}, schema->copy());
    auto logicalExtend =
        make_shared<LogicalExtend>("a._id", 0, "b._id", 0, 0, FWD, false, 1, 1, logicalSelectScan);
    auto logicalSubPlan =
        createLogicalPlan(logicalExtend, createListExtendSchema("a._id", "b._id"));
    // end inner plan
    auto subqueryExpr = make_shared<ExistentialSubqueryExpression>(move(logicalSubPlan));
    auto notExpr = make_shared<Expression>(NOT, BOOL, move(subqueryExpr));
    auto logicalFilter = make_shared<LogicalFilter>(move(notExpr), 0, logicalFlatten);
    auto logicalPlan = createLogicalPlan(logicalFilter, move(schema));

    ASSERT_TRUE(mapAndExecuteLogicalPlan(move(logicalPlan)) == 3);
}

/**
 * MATCH (a:person) WHERE NOT EXISTS {
 *          MATCH (a)-[:studyAt]->(:Organisation)
 *          RETURN *
 *      }
 * RETURN COUNT(*)
 * Plan: SCAN(a)->Flatten(a)->Filter(subPlan)->ResultCollector
 * SubPlan: SelectScan()->ColExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, LogicalSubqueryExistColumnTest) {
    auto schema = createANodeSchema();
    auto logicalFlatten = createBasicScanAndFlattenA();
    // start inner plan
    auto logicalSelectScan =
        make_shared<LogicalSelectScan>(vector<string>{"a._id"}, schema->copy());
    auto logicalExtend =
        make_shared<LogicalExtend>("a._id", 0, "b._id", 1, 1, FWD, true, 1, 1, logicalSelectScan);
    auto logicalSubPlan = createLogicalPlan(logicalExtend, createColExtendSchema("a._id", "b._id"));
    // end inner plan
    auto subqueryExpr = make_shared<ExistentialSubqueryExpression>(move(logicalSubPlan));
    auto notExpr = make_shared<Expression>(NOT, BOOL, move(subqueryExpr));
    auto logicalFilter = make_shared<LogicalFilter>(move(notExpr), 0, logicalFlatten);
    auto logicalPlan = createLogicalPlan(logicalFilter, move(schema));

    ASSERT_TRUE(mapAndExecuteLogicalPlan(move(logicalPlan)) == 5);
}

/**
 * MATCH (a:person) WHERE EXISTS {
 *          MATCH (a)-[:studyAt]->(b:Organisation)
 *      } OR EXISTS {
 *          MATCH (a)-[:workAt]->(c:Organisation)
 *      }
 * Plan: SCAN(a)->Flatten(a)->Filter(leftSubPlan OR rightSubPlan)->ResultCollector
 * LeftSubPlan: SelectScan()->ColExtend(b)->ResultCollector
 * RightSubPlan: SelectScan()->ColExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, LogicalSubqueryExistColumnORTest) {
    auto schema = createANodeSchema();
    auto logicalFlatten = createBasicScanAndFlattenA();
    // start left inner plan
    auto leftLogicalSelectScan =
        make_shared<LogicalSelectScan>(vector<string>{"a._id"}, schema->copy());
    auto leftLogicalExtend = make_shared<LogicalExtend>(
        "a._id", 0, "b._id", 1, 1, FWD, true, 1, 1, leftLogicalSelectScan);
    auto leftLogicalSubPlan =
        createLogicalPlan(leftLogicalExtend, createColExtendSchema("a._id", "b._id"));
    // end left inner plan
    // start right inner plan
    auto rightLogicalSelectScan =
        make_shared<LogicalSelectScan>(vector<string>{"a._id"}, schema->copy());
    auto rightLogicalExtend = make_shared<LogicalExtend>(
        "a._id", 0, "c._id", 1, 2, FWD, true, 1, 1, rightLogicalSelectScan);
    auto rightLogicalSubPlan =
        createLogicalPlan(rightLogicalExtend, createColExtendSchema("a._id", "c._id"));
    // end right inner plan
    auto leftSubqueryExpr = make_shared<ExistentialSubqueryExpression>(move(leftLogicalSubPlan));
    auto rightSubqueryExpr = make_shared<ExistentialSubqueryExpression>(move(rightLogicalSubPlan));
    auto orExpr =
        make_shared<Expression>(OR, BOOL, move(leftSubqueryExpr), move(rightSubqueryExpr));
    auto logicalFilter = make_shared<LogicalFilter>(move(orExpr), 0, logicalFlatten);
    auto logicalPlan = createLogicalPlan(logicalFilter, move(schema));

    ASSERT_TRUE(mapAndExecuteLogicalPlan(move(logicalPlan)) == 6);
}

/**
 * MATCH (a:person) WHERE EXISTS {
 *          MATCH (a)-[:knows]->(b:Person) WHERE EXISTS {
 *              MATCH (b)-[:worksAt]->(c:Organisation)
 *              RETURN *
 *          }
 *          RETURN *
 *      }
 * RETURN COUNT(*)
 * Plan: SCAN(a)->Flatten(a)->Filter(subPlan)->ResultCollector
 * SubPlan: SelectScan(a)->ListExtend(b)->ResultCollector
 * InnerSubPlan: SelectScan(b)->ColExtend(c)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, LogicalNestedSubqueryTest) {
    auto schema = createANodeSchema();
    auto logicalFlatten = createBasicScanAndFlattenA();
    // start inner plan
    auto innerSchema = createListExtendSchema("a._id", "b._id");
    auto innerLogicalSelectScan =
        make_shared<LogicalSelectScan>(vector<string>{"a._id"}, schema->copy());
    auto innerLogicalExtend = make_unique<LogicalExtend>(
        "a._id", 0, "b._id", 0, 0, FWD, false, 1, 1, move(innerLogicalSelectScan));
    auto innerLogicalFlatten = make_shared<LogicalFlatten>("b._id", move(innerLogicalExtend));
    // start inner inner plan
    auto innerInnerLogicalSelectScan =
        make_shared<LogicalSelectScan>(vector<string>{"b._id"}, innerSchema->copy());
    auto innerInnerLogicalExtend = make_shared<LogicalExtend>(
        "b._id", 0, "c._id", 1, 2, FWD, true, 1, 1, move(innerInnerLogicalSelectScan));
    auto innerInnerLogicalSubPlan =
        createLogicalPlan(innerInnerLogicalExtend, createColExtendSchema("b._id", "c._id"));
    // end inner inner plan
    auto innerExpr = make_shared<ExistentialSubqueryExpression>(move(innerInnerLogicalSubPlan));
    auto innerLogicalFilter =
        make_shared<LogicalFilter>(move(innerExpr), 1, move(innerLogicalFlatten));
    auto innerLogicalSubPlan = createLogicalPlan(innerLogicalFilter, move(innerSchema));
    // end inner plan
    auto expr = make_shared<ExistentialSubqueryExpression>(move(innerLogicalSubPlan));
    auto logicalFilter = make_shared<LogicalFilter>(move(expr), 0, move(logicalFlatten));
    auto logicalPlan = createLogicalPlan(logicalFilter, move(schema));

    ASSERT_TRUE(mapAndExecuteLogicalPlan(move(logicalPlan)) == 4);
}
