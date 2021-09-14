#include "gtest/gtest.h"
#include "test/mock/mock_graph.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/node_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/function/include/aggregation/count.h"
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
    countStarAggrEvaluator->evaluate();

    auto countStarState = countStarAggrEvaluator->getFunction()->initialize();
    countStarAggrEvaluator->getFunction()->update(
        (uint8_t*)countStarState.get(), nullptr, resultSet.getNumTuples());
    auto otherCountStarState = countStarAggrEvaluator->getFunction()->initialize();
    *((uint64_t*)otherCountStarState->val.get()) = 10;
    otherCountStarState->isNull = false;
    countStarAggrEvaluator->getFunction()->combine(
        (uint8_t*)countStarState.get(), (uint8_t*)otherCountStarState.get());
    countStarAggrEvaluator->getFunction()->finalize((uint8_t*)countStarState.get());
    ASSERT_EQ(*((uint64_t*)countStarState->val.get()), 110);
}
