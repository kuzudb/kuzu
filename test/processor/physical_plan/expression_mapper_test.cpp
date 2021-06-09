#include "gtest/gtest.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/processor/include/physical_plan/expression_mapper.h"

using namespace graphflow::processor;
using namespace graphflow::binder;

TEST(ExpressionTests, BinaryExpressionEvaluatorTest) {
    auto propertyExpression =
        make_unique<Expression>(ExpressionType::PROPERTY, DataType::INT32, "a.prop");
    auto literal = Value(5);
    auto literalExpression =
        make_unique<LiteralExpression>(ExpressionType::LITERAL_INT, DataType::INT32, literal);

    auto addLogicalOperator = make_unique<Expression>(
        ExpressionType::ADD, DataType::INT32, move(propertyExpression), move(literalExpression));

    auto valueVector = make_shared<ValueVector>(INT32);
    auto values = (int32_t*)valueVector->values;
    for (auto i = 0u; i < 100; i++) {
        values[i] = i;
    }
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 100;
    dataChunk->state->numSelectedValues = 100;
    dataChunk->append(valueVector);

    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto resultSet = ResultSet();
    physicalOperatorInfo.appendAsNewDataChunk("a.prop");
    resultSet.append(dataChunk);

    auto rootExpressionEvaluator =
        ExpressionMapper::mapToPhysical(*addLogicalOperator, physicalOperatorInfo, resultSet);
    rootExpressionEvaluator->evaluate();

    auto results = (int32_t*)rootExpressionEvaluator->result->values;
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(results[i], i + 5);
    }
}

TEST(ExpressionTests, UnaryExpressionEvaluatorTest) {
    auto propertyExpression =
        make_unique<Expression>(ExpressionType::PROPERTY, DataType::INT32, "a.prop");
    auto negateLogicalOperator =
        make_unique<Expression>(ExpressionType::NEGATE, DataType::INT32, move(propertyExpression));

    auto valueVector = make_shared<ValueVector>(INT32);
    auto values = (int32_t*)valueVector->values;
    for (auto i = 0u; i < 100; i++) {
        int32_t value = i;
        if (i % 2ul == 0ul) {
            values[i] = value;
        } else {
            values[i] = -value;
        }
    }
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 100;
    dataChunk->state->numSelectedValues = 100;
    dataChunk->append(valueVector);

    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto resultSet = ResultSet();
    physicalOperatorInfo.appendAsNewDataChunk("a.prop");
    resultSet.append(dataChunk);

    auto rootExpressionEvaluator =
        ExpressionMapper::mapToPhysical(*negateLogicalOperator, physicalOperatorInfo, resultSet);
    rootExpressionEvaluator->evaluate();

    auto results = (int32_t*)rootExpressionEvaluator->result->values;
    for (auto i = 0u; i < 100; i++) {
        int32_t value = i;
        if (i % 2ul == 0ul) {
            ASSERT_EQ(results[i], -value);
        } else {
            ASSERT_EQ(results[i], value);
        }
    }
}
