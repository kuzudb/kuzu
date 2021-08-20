#include "gtest/gtest.h"
#include "test/mock/mock_graph.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/node_expression.h"
#include "src/binder/include/expression/property_expression.h"
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
        schema.appendToGroup("_0_a.prop", groupPos);
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
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->selectedSize = 100;
    dataChunk->append(valueVector);
    auto resultSet = ResultSet();
    resultSet.append(dataChunk);

    auto physicalOperatorInfo = makeSimplePhysicalOperatorInfo();
    auto rootExpressionEvaluator =
        ExpressionMapper(planMapper.get())
            .mapToPhysical(*addLogicalOperator, physicalOperatorInfo, &resultSet, *context);
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
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->selectedSize = 100;
    dataChunk->append(valueVector);
    auto resultSet = ResultSet();
    resultSet.append(dataChunk);

    auto physicalOperatorInfo = makeSimplePhysicalOperatorInfo();
    auto rootExpressionEvaluator =
        ExpressionMapper(planMapper.get())
            .mapToPhysical(*negateLogicalOperator, physicalOperatorInfo, &resultSet, *context);
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
