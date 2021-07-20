#include "gtest/gtest.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/node_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/processor/include/physical_plan/expression_mapper.h"

using namespace graphflow::processor;
using namespace graphflow::binder;

TEST(ExpressionTests, BinaryExpressionEvaluatorTest) {
    auto nodeExpression = make_unique<NodeExpression>("a", 0, 0);
    auto propertyExpression =
        make_unique<PropertyExpression>(DataType::INT64, "prop", 0, move(nodeExpression));
    Literal literal = Literal((int64_t)5);
    auto literalExpression =
        make_unique<LiteralExpression>(ExpressionType::LITERAL_INT, DataType::INT64, literal);

    auto addLogicalOperator = make_unique<Expression>(
        ExpressionType::ADD, DataType::INT64, move(propertyExpression), move(literalExpression));

    auto memoryManager = make_unique<MemoryManager>();
    auto valueVector = make_shared<ValueVector>(memoryManager.get(), INT64);
    auto values = (int64_t*)valueVector->values;
    for (auto i = 0u; i < 100; i++) {
        values[i] = i;
    }
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 100;
    dataChunk->append(valueVector);

    auto schema = Schema();
    auto groupPos = schema.createGroup();
    schema.appendToGroup("_0_a.prop", groupPos);
    auto physicalOperatorInfo = PhysicalOperatorsInfo(schema);
    auto resultSet = ResultSet();
    resultSet.append(dataChunk);

    auto rootExpressionEvaluator = ExpressionMapper::mapToPhysical(
        *memoryManager, *addLogicalOperator, physicalOperatorInfo, resultSet);
    rootExpressionEvaluator->evaluate();

    auto results = (int64_t*)rootExpressionEvaluator->result->values;
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(results[i], i + 5);
    }
}

TEST(ExpressionTests, UnaryExpressionEvaluatorTest) {
    auto nodeExpression = make_unique<NodeExpression>("a", 0, 0);
    auto propertyExpression =
        make_unique<PropertyExpression>(DataType::INT64, "prop", 0, move(nodeExpression));
    auto negateLogicalOperator =
        make_unique<Expression>(ExpressionType::NEGATE, DataType::INT64, move(propertyExpression));

    auto memoryManager = make_unique<MemoryManager>();
    auto valueVector = make_shared<ValueVector>(memoryManager.get(), INT64);
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
    dataChunk->state->size = 100;
    dataChunk->append(valueVector);

    auto schema = Schema();
    auto groupPos = schema.createGroup();
    schema.appendToGroup("_0_a.prop", groupPos);
    auto physicalOperatorInfo = PhysicalOperatorsInfo(schema);
    auto resultSet = ResultSet();
    resultSet.append(dataChunk);

    auto rootExpressionEvaluator = ExpressionMapper::mapToPhysical(
        *memoryManager, *negateLogicalOperator, physicalOperatorInfo, resultSet);
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
