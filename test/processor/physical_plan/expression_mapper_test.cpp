#include "gtest/gtest.h"

#include "src/expression/include/logical/logical_expression.h"
#include "src/processor/include/physical_plan/expression_mapper.h"

using namespace graphflow::processor;
using namespace graphflow::expression;

TEST(ExpressionTests, BinaryPhysicalExpressionTest) {
    auto propertyExpression =
        make_unique<LogicalExpression>(ExpressionType::VARIABLE, DataType::INT, "a.prop");
    auto literal = Literal(5);
    auto literalExpression =
        make_unique<LogicalExpression>(ExpressionType::LITERAL_INT, DataType::INT, literal);

    auto addLogicalOperator = make_unique<LogicalExpression>(
            ExpressionType::ADD, move(propertyExpression), move(literalExpression));

    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto dataChunks = DataChunks();
    physicalOperatorInfo.put("a.prop", 0 /*dataChunkPos*/, 0 /*valueVectorPos*/);

    auto valueVector = make_shared<ValueVector>(INT, 100);
    auto values = (int32_t*)valueVector->getValues();
    for (auto i = 0u; i < 100; i++) {
        values[i] = i;
    }
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->size = 100;
    valueVector->setDataChunkOwner(dataChunk);
    dataChunk->append(valueVector);
    dataChunks.append(dataChunk);

    auto rootPhysicalExpression = ExpressionMapper::mapToPhysical(
        *addLogicalOperator, physicalOperatorInfo, dataChunks);
    rootPhysicalExpression->evaluate();

    auto resultValues = (int32_t*)rootPhysicalExpression->getResult()->getValues();
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(resultValues[i], i + 5);
    }
}

TEST(ExpressionTests, UnaryPhysicalExpressionTest) {
    auto propertyExpression =
        make_unique<LogicalExpression>(ExpressionType::VARIABLE, DataType::INT, "a.prop");
    auto negateLogicalOperator =
        make_unique<LogicalExpression>(ExpressionType::NEGATE, move(propertyExpression));

    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto dataChunks = DataChunks();
    physicalOperatorInfo.put("a.prop", 0 /*dataChunkPos*/, 0 /*valueVectorPos*/);

    auto valueVector = make_shared<ValueVector>(INT, 100);
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
    dataChunk->size = 100;
    valueVector->setDataChunkOwner(dataChunk);
    dataChunk->append(valueVector);
    dataChunks.append(dataChunk);

    auto rootPhysicalExpression = ExpressionMapper::mapToPhysical(
        *negateLogicalOperator, physicalOperatorInfo, dataChunks);
    rootPhysicalExpression->evaluate();

    auto result = rootPhysicalExpression->getResult();
    auto resultValues = (int32_t*)result->getValues();
    for (auto i = 0u; i < 100; i++) {
        int32_t value = i;
        if (i % 2ul == 0ul) {
            ASSERT_EQ(resultValues[i], -value);
        } else {
            ASSERT_EQ(resultValues[i], value);
        }
    }
}
