#include "gtest/gtest.h"
#include "test/mock/mock_graph.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/node_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/function/include/aggregation/avg.h"
#include "src/function/include/aggregation/count.h"
#include "src/function/include/aggregation/min_max.h"
#include "src/function/include/aggregation/sum.h"
#include "src/processor/include/physical_plan/plan_mapper.h"

using ::testing::NiceMock;
using ::testing::Test;

using namespace graphflow::processor;
using namespace graphflow::binder;

class ExpressionMapperTest : public Test {

public:
    void SetUp() override {
        graph.setUp();
        planMapper = make_unique<PlanMapper>(graph);
        profiler = make_unique<Profiler>();
        memoryManager = make_unique<MemoryManager>();
        context =
            make_unique<ExecutionContext>(*profiler, nullptr /*transaction*/, memoryManager.get());
    }

    static unique_ptr<PropertyExpression> makeAPropExpression() {
        auto nodeExpression = make_unique<NodeExpression>("_0_a", 0);
        return make_unique<PropertyExpression>(DataType::INT64, "prop", 0, move(nodeExpression));
    }

    static PhysicalOperatorsInfo makeSimplePhysicalOperatorInfo() {
        auto schema = Schema();
        auto groupPos = schema.createGroup();
        schema.insertToGroup("_0_a.prop", groupPos);
        return PhysicalOperatorsInfo(schema);
    }

public:
    NiceMock<TinySnbGraph> graph;
    unique_ptr<PlanMapper> planMapper;
    unique_ptr<Profiler> profiler;
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<ExecutionContext> context;
};

TEST_F(ExpressionMapperTest, BinaryExpressionEvaluatorTest) {
    Literal literal = Literal((int64_t)5);
    auto literalExpression =
        make_unique<LiteralExpression>(ExpressionType::LITERAL_INT, DataType::INT64, literal);
    auto addLogicalOperator = make_shared<Expression>(
        ExpressionType::ADD, DataType::INT64, makeAPropExpression(), move(literalExpression));

    auto valueVector = make_shared<ValueVector>(context->memoryManager, INT64);
    auto values = (int64_t*)valueVector->values;
    for (auto i = 0u; i < 100; i++) {
        values[i] = i;
    }
    auto dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 100;
    dataChunk->insert(0, valueVector);
    auto resultSet = ResultSet(1);
    resultSet.insert(0, dataChunk);

    auto physicalOperatorInfo = makeSimplePhysicalOperatorInfo();
    auto rootExpressionEvaluator =
        ExpressionMapper(planMapper.get())
            .mapToPhysical(*addLogicalOperator, physicalOperatorInfo, *context);
    rootExpressionEvaluator->initResultSet(resultSet, *memoryManager);
    rootExpressionEvaluator->evaluate();

    auto results = (int64_t*)rootExpressionEvaluator->result->values;
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(results[i], i + 5);
    }
}

TEST_F(ExpressionMapperTest, UnaryExpressionEvaluatorTest) {
    auto negateLogicalOperator =
        make_shared<Expression>(ExpressionType::NEGATE, DataType::INT64, makeAPropExpression());

    auto valueVector = make_shared<ValueVector>(context->memoryManager, INT64);
    auto values = (int64_t*)valueVector->values;
    for (auto i = 0u; i < 100; i++) {
        int64_t value = i;
        if (i % 2ul == 0ul) {
            values[i] = value;
        } else {
            values[i] = -value;
        }
    }
    auto dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 100;
    dataChunk->insert(0, valueVector);
    auto resultSet = ResultSet(1);
    resultSet.insert(0, dataChunk);

    auto physicalOperatorInfo = makeSimplePhysicalOperatorInfo();
    auto rootExpressionEvaluator =
        ExpressionMapper(planMapper.get())
            .mapToPhysical(*negateLogicalOperator, physicalOperatorInfo, *context);
    rootExpressionEvaluator->initResultSet(resultSet, *memoryManager);
    rootExpressionEvaluator->evaluate();

    auto results = (int64_t*)rootExpressionEvaluator->result->values;
    for (auto i = 0u; i < 100; i++) {
        int64_t value = i;
        if (i % 2ul == 0ul) {
            ASSERT_EQ(results[i], -value);
        } else {
            ASSERT_EQ(results[i], value);
        }
    }
}

TEST_F(ExpressionMapperTest, AggrExpressionEvaluatorTest) {
    auto physicalOperatorInfo = makeSimplePhysicalOperatorInfo();
    auto valueVector = make_shared<ValueVector>(context->memoryManager, INT64);
    auto dataChunk = make_shared<DataChunk>(1);
    auto values = (int64_t*)valueVector->values;
    for (auto i = 0u; i < 100; i++) {
        values[i] = i;
        if (i % 2 == 0) {
            valueVector->setNull(i, true /* isNull */);
        }
    }
    dataChunk->state->selectedSize = 100;
    dataChunk->insert(0, valueVector);
    auto resultSet = ResultSet(1);
    resultSet.insert(0, dataChunk);

    auto countStarExpr = make_unique<Expression>(ExpressionType::COUNT_STAR_FUNC, DataType::INT64);
    auto countStarExprEvaluator =
        ExpressionMapper(planMapper.get())
            .mapToPhysical(*countStarExpr, physicalOperatorInfo, *context);
    auto countStarAggrEvaluator =
        reinterpret_cast<AggregateExpressionEvaluator*>(countStarExprEvaluator.get());
    countStarAggrEvaluator->initResultSet(resultSet, *memoryManager);

    auto countStarState = make_unique<CountFunction<true>::CountState>();
    countStarAggrEvaluator->getFunction()->initialize((uint8_t*)countStarState.get());
    countStarAggrEvaluator->getFunction()->update(
        (uint8_t*)countStarState.get(), nullptr, resultSet.getNumTuples());
    auto otherCountStarState = make_unique<CountFunction<true>::CountState>();
    otherCountStarState->val = 10;
    countStarAggrEvaluator->getFunction()->combine(
        (uint8_t*)countStarState.get(), (uint8_t*)otherCountStarState.get());
    auto countStarFinalState = make_unique<CountFunction<true>::CountState>();
    countStarAggrEvaluator->getFunction()->finalize(
        (uint8_t*)countStarState.get(), (uint8_t*)countStarFinalState.get());
    ASSERT_EQ(countStarFinalState->val, 110);

    auto countExpr =
        make_unique<Expression>(ExpressionType::COUNT_FUNC, DataType::INT64, makeAPropExpression());
    auto countExprEvaluator = ExpressionMapper(planMapper.get())
                                  .mapToPhysical(*countExpr, physicalOperatorInfo, *context);
    auto countAggrEvaluator =
        reinterpret_cast<AggregateExpressionEvaluator*>(countExprEvaluator.get());
    countStarAggrEvaluator->initResultSet(resultSet, *memoryManager);

    auto countState = make_unique<CountFunction<false>::CountState>();
    countAggrEvaluator->getFunction()->initialize((uint8_t*)countState.get());
    countAggrEvaluator->getFunction()->update(
        (uint8_t*)countState.get(), valueVector.get(), valueVector->state->selectedSize);
    auto otherCountState = make_unique<CountFunction<false>::CountState>();
    otherCountState->val = 10;
    countAggrEvaluator->getFunction()->combine(
        (uint8_t*)countState.get(), (uint8_t*)otherCountState.get());
    auto countFinalState = make_unique<CountFunction<false>::CountState>();
    countAggrEvaluator->getFunction()->finalize(
        (uint8_t*)countState.get(), (uint8_t*)countFinalState.get());
    ASSERT_EQ(countFinalState->val, 60);

    auto sumExpr =
        make_unique<Expression>(ExpressionType::SUM_FUNC, DataType::INT64, makeAPropExpression());
    auto sumExprEvaluator =
        ExpressionMapper(planMapper.get()).mapToPhysical(*sumExpr, physicalOperatorInfo, *context);
    auto sumAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(sumExprEvaluator.get());
    sumAggrEvaluator->initResultSet(resultSet, *memoryManager);

    auto sumState = make_unique<SumFunction<int64_t>::SumState>();
    sumAggrEvaluator->getFunction()->initialize((uint8_t*)sumState.get());
    sumAggrEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), valueVector.get(), valueVector->state->selectedSize);
    auto otherSumState = make_unique<SumFunction<int64_t>::SumState>();
    otherSumState->val = 10;
    sumAggrEvaluator->getFunction()->combine(
        (uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    auto sumFinalState = make_unique<SumFunction<int64_t>::SumState>();
    sumAggrEvaluator->getFunction()->finalize(
        (uint8_t*)sumState.get(), (uint8_t*)sumFinalState.get());
    auto sumValue = otherSumState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumFinalState->val, sumValue);

    auto avgExpr =
        make_unique<Expression>(ExpressionType::AVG_FUNC, DataType::INT64, makeAPropExpression());
    auto avgExprEvaluator =
        ExpressionMapper(planMapper.get()).mapToPhysical(*avgExpr, physicalOperatorInfo, *context);
    auto avgAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(avgExprEvaluator.get());
    avgAggrEvaluator->initResultSet(resultSet, *memoryManager);

    auto avgState = make_unique<AvgFunction<int64_t>::AvgState>();
    avgAggrEvaluator->getFunction()->initialize((uint8_t*)avgState.get());
    avgAggrEvaluator->getFunction()->update(
        (uint8_t*)avgState.get(), valueVector.get(), valueVector->state->selectedSize);
    auto otherAvgState = make_unique<AvgFunction<int64_t>::AvgState>();
    otherAvgState->val = 10;
    otherAvgState->numValues = 1;
    avgAggrEvaluator->getFunction()->combine(
        (uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    auto avgFinalState = make_unique<AvgFunction<double_t>::AvgState>();
    avgAggrEvaluator->getFunction()->finalize(
        (uint8_t*)avgState.get(), (uint8_t*)avgFinalState.get());
    ASSERT_EQ(avgFinalState->val, (double_t)(sumValue) / (double_t)51);

    auto maxExpr =
        make_unique<Expression>(ExpressionType::MAX_FUNC, DataType::INT64, makeAPropExpression());
    auto maxExprEvaluator =
        ExpressionMapper(planMapper.get()).mapToPhysical(*maxExpr, physicalOperatorInfo, *context);
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(maxExprEvaluator.get());
    maxAggrEvaluator->initResultSet(resultSet, *memoryManager);

    auto maxState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->initialize((uint8_t*)maxState.get());
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), valueVector.get(), valueVector->state->selectedSize);
    auto otherMaxState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    otherMaxState->val = 101;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    auto maxFinalState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->finalize(
        (uint8_t*)maxState.get(), (uint8_t*)maxFinalState.get());
    ASSERT_EQ(maxFinalState->val, 101);
}
