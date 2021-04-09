#include "gtest/gtest.h"

#include "src/expression/include/logical/logical_expression.h"
#include "src/processor/include/physical_plan/expression_mapper.h"

using namespace graphflow::processor;
using namespace graphflow::expression;

TEST(ExpressionTests, BinaryPhysicalExpressionTest) {
    auto propertyExpression =
        make_unique<LogicalExpression>(ExpressionType::PROPERTY, DataType::INT32, "a.prop");
    auto literal = Literal(5);
    auto literalExpression =
        make_unique<LogicalExpression>(ExpressionType::LITERAL_INT, DataType::INT32, literal);

    auto addLogicalOperator = make_unique<LogicalExpression>(
        ExpressionType::ADD, DataType::INT32, move(propertyExpression), move(literalExpression));

    auto valueVector = make_shared<ValueVector>(INT32, 100);
    auto values = (int32_t*)valueVector->getValues();
    for (auto i = 0u; i < 100; i++) {
        values[i] = i;
    }
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 100;
    dataChunk->state->numSelectedValues = 100;
    dataChunk->append(valueVector);

    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    physicalOperatorInfo.appendAsNewDataChunk("a.prop");

    auto rootPhysicalExpression =
        ExpressionMapper::mapToPhysical(*addLogicalOperator, physicalOperatorInfo);
    rootPhysicalExpression->setExpressionInputDataChunk(dataChunk);
    rootPhysicalExpression->setExpressionResultOwnerState(dataChunk->state);
    rootPhysicalExpression->evaluate();

    auto results = (int32_t*)rootPhysicalExpression->result->getValues();
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(results[i], i + 5);
    }
}

TEST(ExpressionTests, UnaryPhysicalExpressionTest) {
    auto propertyExpression =
        make_unique<LogicalExpression>(ExpressionType::PROPERTY, DataType::INT32, "a.prop");
    auto negateLogicalOperator = make_unique<LogicalExpression>(
        ExpressionType::NEGATE, DataType::INT32, move(propertyExpression));

    auto valueVector = make_shared<ValueVector>(INT32, 100);
    auto values = (int32_t*)valueVector->getValues();
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
    physicalOperatorInfo.appendAsNewDataChunk("a.prop");

    auto rootPhysicalExpression =
        ExpressionMapper::mapToPhysical(*negateLogicalOperator, physicalOperatorInfo);
    rootPhysicalExpression->setExpressionInputDataChunk(dataChunk);
    rootPhysicalExpression->setExpressionResultOwnerState(dataChunk->state);
    rootPhysicalExpression->evaluate();

    auto results = (int32_t*)rootPhysicalExpression->result->getValues();
    for (auto i = 0u; i < 100; i++) {
        int32_t value = i;
        if (i % 2ul == 0ul) {
            ASSERT_EQ(results[i], -value);
        } else {
            ASSERT_EQ(results[i], value);
        }
    }
}
