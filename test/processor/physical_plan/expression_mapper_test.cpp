#include "gtest/gtest.h"
#include "test/mock/mock_graph.h"

#include "src/binder/expression/include/literal_expression.h"
#include "src/binder/expression/include/node_expression.h"
#include "src/function/include/aggregate/count.h"
#include "src/processor/include/physical_plan/mapper/expression_mapper.h"

using ::testing::NiceMock;
using ::testing::Test;

using namespace graphflow::processor;
using namespace graphflow::binder;
using namespace graphflow::function;

class ExpressionMapperTest : public Test {

public:
    void SetUp() override {
        graph.setUp();
        profiler = make_unique<Profiler>();
        memoryManager = make_unique<MemoryManager>();
        context = make_unique<ExecutionContext>(*profiler, memoryManager.get());
    }

    static unique_ptr<PropertyExpression> makeAPropExpression() {
        auto nodeExpression = make_unique<NodeExpression>("_0_a", 0);
        return make_unique<PropertyExpression>(DataType::INT64, "prop", 0, move(nodeExpression));
    }

    static MapperContext makeSimpleMapperContext() {
        auto schema = Schema();
        auto groupPos = schema.createGroup();
        schema.insertToGroup("_0_a.prop", groupPos);
        auto context = MapperContext(make_unique<ResultSetDescriptor>(schema));
        context.addComputedExpressions("_0_a.prop");
        return context;
    }

public:
    NiceMock<TinySnbGraph> graph;
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

    auto mapperContext = makeSimpleMapperContext();
    auto rootExpressionEvaluator = ExpressionMapper().mapLogicalExpressionToPhysical(
        *addLogicalOperator, mapperContext, *context);
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

    auto mapperContext = makeSimpleMapperContext();
    auto rootExpressionEvaluator = ExpressionMapper().mapLogicalExpressionToPhysical(
        *negateLogicalOperator, mapperContext, *context);
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
