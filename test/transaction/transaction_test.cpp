#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class TransactionTests : public BaseGraphLoadingTest {
public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 22);
        createConn();
        writeTrx = database->getTransactionManager()->beginWriteTransaction();
        readTrx = database->getTransactionManager()->beginReadOnlyTransaction();

        label_t personNodeLabel = database->getCatalog()->getNodeLabelFromName("person");
        uint32_t agePropertyID =
            database->getCatalog()->getNodeProperty(personNodeLabel, "age").propertyID;
        uint32_t eyeSightPropertyID =
            database->getCatalog()->getNodeProperty(personNodeLabel, "eyeSight").propertyID;

        dataChunk = make_shared<DataChunk>(3);
        nodeVector = make_shared<ValueVector>(database->getMemoryManager(), NODE);
        dataChunk->insert(0, nodeVector);
        ((nodeID_t*)nodeVector->values)[0].offset = 0;
        ((nodeID_t*)nodeVector->values)[1].offset = 1;

        agePropertyVectorToReadDataInto =
            make_shared<ValueVector>(database->getMemoryManager(), INT64);
        dataChunk->insert(1, agePropertyVectorToReadDataInto);
        eyeSightVectorToReadDataInto =
            make_shared<ValueVector>(database->getMemoryManager(), DOUBLE);
        dataChunk->insert(2, eyeSightVectorToReadDataInto);

        personAgeColumn = database->getStorageManager()->getNodesStore().getNodePropertyColumn(
            personNodeLabel, agePropertyID);

        personEyeSightColumn = database->getStorageManager()->getNodesStore().getNodePropertyColumn(
            personNodeLabel, eyeSightPropertyID);
    }

    void readAndAssertAgePropertyNode(
        uint64_t nodeOffset, Transaction* trx, int64_t expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        personAgeColumn->readValues(trx, nodeVector, agePropertyVectorToReadDataInto);
        if (isNull) {
            ASSERT_TRUE(agePropertyVectorToReadDataInto->isNull(dataChunk->state->currIdx));
        } else {
            ASSERT_FALSE(agePropertyVectorToReadDataInto->isNull(dataChunk->state->currIdx));
            ASSERT_EQ(expectedValue,
                ((uint64_t*)agePropertyVectorToReadDataInto->values)[dataChunk->state->currIdx]);
        }
    }

    void readAndAssertEyeSightPropertyNode(
        uint64_t nodeOffset, Transaction* trx, double expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        personEyeSightColumn->readValues(trx, nodeVector, eyeSightVectorToReadDataInto);
        if (isNull) {
            ASSERT_TRUE(eyeSightVectorToReadDataInto->isNull(dataChunk->state->currIdx));
        } else {
            ASSERT_FALSE(eyeSightVectorToReadDataInto->isNull(dataChunk->state->currIdx));
            ASSERT_EQ(expectedValue,
                ((double*)eyeSightVectorToReadDataInto->values)[dataChunk->state->currIdx]);
        }
    }

    void writeToAgePropertyNode(uint64_t nodeOffset, int64_t expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        auto propertyVectorToWriteDataTo =
            make_shared<ValueVector>(database->getMemoryManager(), INT64);
        if (isNull) {
            propertyVectorToWriteDataTo->setNull(dataChunk->state->currIdx, true /* is null */);
        } else {
            propertyVectorToWriteDataTo->setNull(
                dataChunk->state->currIdx, false /* is not null */);
            ((uint64_t*)propertyVectorToWriteDataTo->values)[dataChunk->state->currIdx] =
                expectedValue;
        }
        personAgeColumn->writeValueForFlatVector(
            writeTrx.get(), nodeVector, propertyVectorToWriteDataTo);
    }

    void writeToEyeSightPropertyNode(uint64_t nodeOffset, double expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        auto propertyVectorToWriteDataTo =
            make_shared<ValueVector>(database->getMemoryManager(), DOUBLE);
        if (isNull) {
            propertyVectorToWriteDataTo->setNull(dataChunk->state->currIdx, true /* is null */);
        } else {
            propertyVectorToWriteDataTo->setNull(
                dataChunk->state->currIdx, false /* is not null */);
            ((double*)propertyVectorToWriteDataTo->values)[dataChunk->state->currIdx] =
                expectedValue;
        }
        personEyeSightColumn->writeValueForFlatVector(
            writeTrx.get(), nodeVector, propertyVectorToWriteDataTo);
    }

public:
    unique_ptr<Transaction> writeTrx;
    unique_ptr<Transaction> readTrx;
    shared_ptr<DataChunk> dataChunk;
    shared_ptr<ValueVector> nodeVector;
    shared_ptr<ValueVector> agePropertyVectorToReadDataInto;
    shared_ptr<ValueVector> eyeSightVectorToReadDataInto;
    Column* personAgeColumn;
    Column* personEyeSightColumn;
};

TEST_F(TransactionTests, SingleTransactionReadWriteToStructuredNodePropertyNonNullTest) {
    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(), 35, false /* is not null */);
    writeToAgePropertyNode(0 /* node offset */, 70, false /* is not null */);
    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(), 70, false /* is not null */);
}

TEST_F(TransactionTests, SingleTransactionReadWriteToStructuredNodePropertyNullTest) {
    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(), 35, false /* is not null */);
    writeToAgePropertyNode(
        0 /* node offset */, 12345 /* this argument is ignored */, true /* is null */);
    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(),
        888 /* this argument is ignored */, true /* is null */);
}

TEST_F(TransactionTests, SingleTransactionReadWriteToMultipleStructuredNodePropertiesTest) {
    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(), 35, false /* is not null */);
    readAndAssertEyeSightPropertyNode(
        0 /* node offset */, writeTrx.get(), 5.0, false /* is not null */);

    writeToAgePropertyNode(
        0 /* node offset */, 12345 /* this argument is ignored */, true /* is null */);
    writeToEyeSightPropertyNode(0 /* node offset */, 0.4, false /* is not null */);

    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(),
        888 /* this argument is ignored */, true /* is null */);
    readAndAssertEyeSightPropertyNode(
        0 /* node offset */, writeTrx.get(), 0.4, false /* is not null */);
}

TEST_F(TransactionTests, Concurrent1Write1ReadTransaction) {
    readAndAssertAgePropertyNode(0 /* node offset */, readTrx.get(), 35, false /* is not null */);
    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(), 35, false /* is not null */);

    readAndAssertAgePropertyNode(1 /* node offset */, readTrx.get(), 30, false /* is not null */);
    readAndAssertAgePropertyNode(1 /* node offset */, writeTrx.get(), 30, false /* is not null */);

    // Change the value of node 0 to 70 and 1 null;
    writeToAgePropertyNode(0, 70, false /* is not null */);
    writeToAgePropertyNode(1, 124124 /* this argument is ignored */, true /* is null */);

    readAndAssertAgePropertyNode(0 /* node offset */, writeTrx.get(), 70, false /* is not null */);
    readAndAssertAgePropertyNode(0 /* node offset */, readTrx.get(), 35, false /* is not null */);

    readAndAssertAgePropertyNode(1 /* node offset */, readTrx.get(), 30, false /* is not null */);
    readAndAssertAgePropertyNode(1 /* node offset */, writeTrx.get(),
        1232532 /* this argument is ignored */, true /* is  null */);
}
