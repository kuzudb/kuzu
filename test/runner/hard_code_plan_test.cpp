#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

#include "src/expression_evaluator/include/binary_expression_evaluator.h"
#include "src/expression_evaluator/include/exist_subquery_evaluator.h"
#include "src/expression_evaluator/include/unary_expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/flatten/flatten.h"
#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/scan_node_id/select_scan.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

using ::testing::Test;
using namespace graphflow::testing;
using namespace graphflow::evaluator;

class TinySnbHardCodePlanTest : public DBLoadedTest {
public:
    void SetUp() override {
        DBLoadedTest::SetUp();
        profiler = make_unique<Profiler>();
        memoryManager = make_unique<MemoryManager>();
        context =
            make_unique<ExecutionContext>(*profiler, nullptr /*transaction*/, memoryManager.get());
        operatorId = 0;
    }

    static unique_ptr<Flatten> getSimpleScanAndFlatten(
        ExecutionContext& context, uint32_t& operatorId) {
        auto morselDesc = make_shared<MorselsDesc>(0, 8);
        auto scan = make_unique<ScanNodeID>(morselDesc, context, operatorId++);
        return make_unique<Flatten>(0, move(scan), context, operatorId++);
    }

    // person - knows -> person
    static unique_ptr<ResultCollector> getSimpleKnowsExtendInnerQuery(
        const ResultSet& outerResultSet, vector<pair<uint64_t, uint64_t>> vectorsToSelect,
        const Graph& graph, ExecutionContext& context, uint32_t& operatorId) {
        auto selectScan =
            make_unique<SelectScan>(outerResultSet, move(vectorsToSelect), context, operatorId++);
        auto extend = make_unique<AdjListExtend>(0, 0, graph.getRelsStore().getAdjLists(FWD, 0, 0),
            0, move(selectScan), context, operatorId++);
        auto innerResultCollector = make_unique<ResultCollector>(
            move(extend), RESULT_COLLECTOR, context, operatorId++, false);
        return innerResultCollector;
    }

    // person - studyAt/workAt -> org
    static unique_ptr<ResultCollector> getSimpleStudyAtExtendInnerQuery(
        const ResultSet& outerResultSet, vector<pair<uint64_t, uint64_t>> vectorsToSelect,
        const Graph& graph, ExecutionContext& context, uint32_t& operatorId, label_t relLabel) {
        auto selectScan =
            make_unique<SelectScan>(outerResultSet, move(vectorsToSelect), context, operatorId++);
        auto extend =
            make_unique<AdjColumnExtend>(0, 0, graph.getRelsStore().getAdjColumn(FWD, 0, relLabel),
                1, move(selectScan), context, operatorId++);
        auto innerResultCollector = make_unique<ResultCollector>(
            move(extend), RESULT_COLLECTOR, context, operatorId++, false);
        return innerResultCollector;
    }

    static unique_ptr<ExpressionEvaluator> getNotExistEvaluator(
        MemoryManager& memoryManager, unique_ptr<ResultCollector> sink) {
        auto subqueryEvaluator = make_unique<ExistSubqueryEvaluator>(memoryManager, move(sink));
        return make_unique<UnaryExpressionEvaluator>(
            memoryManager, move(subqueryEvaluator), NOT, BOOL, true);
    }

    string getInputCSVDir() override { return "dataset/tinysnb/"; }

public:
    unique_ptr<Profiler> profiler;
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<ExecutionContext> context;
    uint32_t operatorId;
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
TEST_F(TinySnbHardCodePlanTest, SubqueryExistListTest) {
    auto& graph = *defaultSystem->graph;

    auto flatten = TinySnbHardCodePlanTest::getSimpleScanAndFlatten(*context, operatorId);
    //--- start inner subquery
    auto vectorsToSelect = vector<pair<uint64_t, uint64_t>>{{0, 0}};
    auto innerResultCollector = TinySnbHardCodePlanTest::getSimpleKnowsExtendInnerQuery(
        *flatten->getResultSet(), move(vectorsToSelect), graph, *context, operatorId);
    //--- end inner subquery
    auto subqueryEvaluator =
        make_unique<ExistSubqueryEvaluator>(*memoryManager, move(innerResultCollector));
    auto filter =
        make_unique<Filter>(move(subqueryEvaluator), 0, move(flatten), *context, operatorId++);
    auto resultCollector =
        make_unique<ResultCollector>(move(filter), RESULT_COLLECTOR, *context, operatorId++, false);

    resultCollector->execute();
    ASSERT_TRUE(resultCollector->queryResult->numTuples == 5);
}

/**
 * MATCH (a:person) WHERE NOT EXISTS {
 *          MATCH (a)-[:knows]->(:Person)
 *          RETURN *
 *      }
 * RETURN COUNT(*)
 * Plan: SCAN(a)->Flatten(a)->Filter(subPlan)->ResultCollector
 * SubPlan: SelectScan()->ListExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, SubqueryNotExistListTest) {
    auto& graph = *defaultSystem->graph;

    auto flatten = TinySnbHardCodePlanTest::getSimpleScanAndFlatten(*context, operatorId);
    //--- start inner subquery
    auto vectorsToSelect = vector<pair<uint64_t, uint64_t>>{{0, 0}};
    auto innerResultCollector = TinySnbHardCodePlanTest::getSimpleKnowsExtendInnerQuery(
        *flatten->getResultSet(), move(vectorsToSelect), graph, *context, operatorId);
    //--- end inner subquery
    auto notEvaluator =
        TinySnbHardCodePlanTest::getNotExistEvaluator(*memoryManager, move(innerResultCollector));
    auto filter = make_unique<Filter>(move(notEvaluator), 0, move(flatten), *context, operatorId++);
    auto resultCollector =
        make_unique<ResultCollector>(move(filter), RESULT_COLLECTOR, *context, operatorId++, false);

    resultCollector->execute();
    ASSERT_TRUE(resultCollector->queryResult->numTuples == 3);
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
TEST_F(TinySnbHardCodePlanTest, SubqueryExistColumnTest) {
    auto& graph = *defaultSystem->graph;

    auto flatten = TinySnbHardCodePlanTest::getSimpleScanAndFlatten(*context, operatorId);
    //--- start inner subquery
    auto vectorsToSelect = vector<pair<uint64_t, uint64_t>>{{0, 0}};
    auto innerResultCollector = TinySnbHardCodePlanTest::getSimpleStudyAtExtendInnerQuery(
        *flatten->getResultSet(), move(vectorsToSelect), graph, *context, operatorId, 1);
    //--- end inner subquery
    auto notEvaluator =
        TinySnbHardCodePlanTest::getNotExistEvaluator(*memoryManager, move(innerResultCollector));
    auto filter = make_unique<Filter>(move(notEvaluator), 0, move(flatten), *context, operatorId++);
    auto resultCollector =
        make_unique<ResultCollector>(move(filter), RESULT_COLLECTOR, *context, operatorId++, false);

    resultCollector->execute();
    ASSERT_TRUE(resultCollector->queryResult->numTuples == 5);
}

/**
 * MATCH (a:person) WHERE EXISTS {
 *          MATCH (a)-[:studyAt]->(:Organisation)
 *      } OR EXISTS {
 *          MATCH (a)-[:workAt]->(:Organisation)
 *      }
 * Plan: SCAN(a)->Flatten(a)->Filter(leftSubPlan OR rightSubPlan)->ResultCollector
 * LeftSubPlan: SelectScan()->ColExtend(b)->ResultCollector
 * RightSubPlan: SelectScan()->ColExtend(b)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, SubqueryExistColumnORTest) {
    auto& graph = *defaultSystem->graph;

    auto flatten = TinySnbHardCodePlanTest::getSimpleScanAndFlatten(*context, operatorId);
    //--- start left inner subquery
    auto leftVectorsToSelect = vector<pair<uint64_t, uint64_t>>{{0, 0}};
    auto leftInnerResultCollector = TinySnbHardCodePlanTest::getSimpleStudyAtExtendInnerQuery(
        *flatten->getResultSet(), move(leftVectorsToSelect), graph, *context, operatorId, 1);
    //--- end left inner subquery
    //--- start right inner subquery
    auto rightVectorsToSelect = vector<pair<uint64_t, uint64_t>>{{0, 0}};
    auto rightInnerResultCollector = TinySnbHardCodePlanTest::getSimpleStudyAtExtendInnerQuery(
        *flatten->getResultSet(), move(rightVectorsToSelect), graph, *context, operatorId, 2);
    //--- end right inner subquery
    auto leftSubqueryEvaluator =
        make_unique<ExistSubqueryEvaluator>(*memoryManager, move(leftInnerResultCollector));
    auto rightSubqueryEvaluator =
        make_unique<ExistSubqueryEvaluator>(*memoryManager, move(rightInnerResultCollector));
    auto orEvaluator = make_unique<BinaryExpressionEvaluator>(
        *memoryManager, move(leftSubqueryEvaluator), move(rightSubqueryEvaluator), OR, BOOL, true);
    auto filter = make_unique<Filter>(move(orEvaluator), 0, move(flatten), *context, operatorId++);
    auto resultCollector =
        make_unique<ResultCollector>(move(filter), RESULT_COLLECTOR, *context, operatorId++, false);

    resultCollector->execute();
    ASSERT_TRUE(resultCollector->queryResult->numTuples == 6);
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
 * Plan: SCAN(a)->Flatten(a)->Filter(leftSubPlan OR rightSubPlan)->ResultCollector
 * SubPlan: SelectScan(a)->ListExtend(b)->ResultCollector
 * InnerSubPlan: SelectScan(b)->ColExtend(c)->ResultCollector
 */
TEST_F(TinySnbHardCodePlanTest, NestedSubqueryTest) {
    auto& graph = *defaultSystem->graph;

    auto flatten = TinySnbHardCodePlanTest::getSimpleScanAndFlatten(*context, operatorId);
    //--- start inner subquery
    auto innerVectorsToSelect = vector<pair<uint64_t, uint64_t>>{{0, 0}};
    auto innerScan = make_unique<SelectScan>(
        *flatten->getResultSet(), move(innerVectorsToSelect), *context, operatorId++);
    auto innerExtend = make_unique<AdjListExtend>(0, 0, graph.getRelsStore().getAdjLists(FWD, 0, 0),
        0, move(innerScan), *context, operatorId++);
    auto innerFlatten = make_unique<Flatten>(1, move(innerExtend), *context, operatorId++);
    //------ start inner inner subquery
    auto innerInnerVectorsToSelect = vector<pair<uint64_t, uint64_t>>{{1, 0}};
    auto innerInnerResultCollector =
        TinySnbHardCodePlanTest::getSimpleStudyAtExtendInnerQuery(*innerFlatten->getResultSet(),
            move(innerInnerVectorsToSelect), graph, *context, operatorId, 2);
    //------ end inner inner subquery
    auto innerSubqueryEvaluator =
        make_unique<ExistSubqueryEvaluator>(*memoryManager, move(innerInnerResultCollector));
    auto innerFilter = make_unique<Filter>(
        move(innerSubqueryEvaluator), 1, move(innerFlatten), *context, operatorId++);
    auto innerResultCollector = make_unique<ResultCollector>(
        move(innerFilter), RESULT_COLLECTOR, *context, operatorId++, false);
    //--- end inner subquery
    auto subqueryEvaluator =
        make_unique<ExistSubqueryEvaluator>(*memoryManager, move(innerResultCollector));
    auto filter =
        make_unique<Filter>(move(subqueryEvaluator), 0, move(flatten), *context, operatorId++);
    auto resultCollector =
        make_unique<ResultCollector>(move(filter), RESULT_COLLECTOR, *context, operatorId++, false);

    resultCollector->execute();
    ASSERT_TRUE(resultCollector->queryResult->numTuples == 4);
}
