#include "common/configs.h"
#include "test_helper/test_helper.h"

using namespace kuzu::testing;

class TransactionTests : public DBTest {
public:
    void SetUp() override {
        DBTest::SetUp();
        initWithoutLoadingGraph();
    }

    string getInputCSVDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }

    void initWithoutLoadingGraph() {
        systemConfig->largePageBufferPoolSize = (1ull << 22);
        // Note we do not actually use the connection field in these tests. We only need the
        // database.
        createDBAndConn();
        writeTrx = getTransactionManager(*database)->beginWriteTransaction();
        readTrx = getTransactionManager(*database)->beginReadOnlyTransaction();

        table_id_t personTableID =
            getCatalog(*database)->getReadOnlyVersion()->getNodeTableIDFromName("person");
        uint32_t agePropertyID = getCatalog(*database)
                                     ->getReadOnlyVersion()
                                     ->getNodeProperty(personTableID, "age")
                                     .propertyID;
        uint32_t eyeSightPropertyID = getCatalog(*database)
                                          ->getReadOnlyVersion()
                                          ->getNodeProperty(personTableID, "eyeSight")
                                          .propertyID;

        dataChunk = make_shared<DataChunk>(3);
        nodeVector = make_shared<ValueVector>(NODE_ID, getMemoryManager(*database));
        dataChunk->insert(0, nodeVector);
        ((nodeID_t*)nodeVector->getData())[0].offset = 0;
        ((nodeID_t*)nodeVector->getData())[1].offset = 1;

        agePropertyVectorToReadDataInto =
            make_shared<ValueVector>(INT64, getMemoryManager(*database));
        dataChunk->insert(1, agePropertyVectorToReadDataInto);
        eyeSightVectorToReadDataInto =
            make_shared<ValueVector>(DOUBLE, getMemoryManager(*database));
        dataChunk->insert(2, eyeSightVectorToReadDataInto);

        personAgeColumn = getStorageManager(*database)->getNodesStore().getNodePropertyColumn(
            personTableID, agePropertyID);

        personEyeSightColumn = getStorageManager(*database)->getNodesStore().getNodePropertyColumn(
            personTableID, eyeSightPropertyID);
    }

    void readAndAssertAgePropertyNode(
        uint64_t nodeOffset, Transaction* trx, int64_t expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        personAgeColumn->read(trx, nodeVector, agePropertyVectorToReadDataInto);
        if (isNull) {
            ASSERT_TRUE(agePropertyVectorToReadDataInto->isNull(dataChunk->state->currIdx));
        } else {
            ASSERT_FALSE(agePropertyVectorToReadDataInto->isNull(dataChunk->state->currIdx));
            ASSERT_EQ(expectedValue,
                agePropertyVectorToReadDataInto->getValue<uint64_t>(dataChunk->state->currIdx));
        }
    }

    void readAndAssertEyeSightPropertyNode(
        uint64_t nodeOffset, Transaction* trx, double expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        personEyeSightColumn->read(trx, nodeVector, eyeSightVectorToReadDataInto);
        if (isNull) {
            ASSERT_TRUE(eyeSightVectorToReadDataInto->isNull(dataChunk->state->currIdx));
        } else {
            ASSERT_FALSE(eyeSightVectorToReadDataInto->isNull(dataChunk->state->currIdx));
            ASSERT_EQ(expectedValue,
                eyeSightVectorToReadDataInto->getValue<double_t>(dataChunk->state->currIdx));
        }
    }

    void writeToAgePropertyNode(uint64_t nodeOffset, int64_t expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        auto propertyVectorToWriteDataTo =
            make_shared<ValueVector>(INT64, getMemoryManager(*database));
        propertyVectorToWriteDataTo->state = dataChunk->state;
        if (isNull) {
            propertyVectorToWriteDataTo->setNull(dataChunk->state->currIdx, true /* is null */);
        } else {
            propertyVectorToWriteDataTo->setNull(
                dataChunk->state->currIdx, false /* is not null */);
            propertyVectorToWriteDataTo->setValue(
                dataChunk->state->currIdx, (uint64_t)expectedValue);
        }
        personAgeColumn->writeValues(nodeVector, propertyVectorToWriteDataTo);
    }

    void writeToEyeSightPropertyNode(uint64_t nodeOffset, double expectedValue, bool isNull) {
        dataChunk->state->currIdx = nodeOffset;
        auto propertyVectorToWriteDataTo =
            make_shared<ValueVector>(DOUBLE, getMemoryManager(*database));
        propertyVectorToWriteDataTo->state = dataChunk->state;
        if (isNull) {
            propertyVectorToWriteDataTo->setNull(dataChunk->state->currIdx, true /* is null */);
        } else {
            propertyVectorToWriteDataTo->setNull(
                dataChunk->state->currIdx, false /* is not null */);
            propertyVectorToWriteDataTo->setValue(
                dataChunk->state->currIdx, (double_t)expectedValue);
        }
        personEyeSightColumn->writeValues(nodeVector, propertyVectorToWriteDataTo);
    }

    void assertOriginalAgeAndEyeSightPropertiesForNodes0And1(Transaction* transaction) {
        readAndAssertAgePropertyNode(0 /* node offset */, transaction, 35, false /* is not null */);
        readAndAssertAgePropertyNode(1 /* node offset */, transaction, 30, false /* is not null */);
        readAndAssertEyeSightPropertyNode(
            0 /* node offset */, transaction, 5.0, false /* is not null */);
        readAndAssertEyeSightPropertyNode(
            1 /* node offset */, transaction, 5.1, false /* is not null */);
    }

    void updateAgeAndEyeSightPropertiesForNodes0And1() {
        // Change the age value of node 0 to 70 and 1 null;
        writeToAgePropertyNode(0, 70, false /* is not null */);
        writeToAgePropertyNode(1, 124124 /* this argument is ignored */, true /* is null */);

        // Change the value of eyeSight property for node 0 to 0.4 and 1 to null;
        writeToEyeSightPropertyNode(0 /* node offset */, 0.4, false /* is not null */);
        writeToEyeSightPropertyNode(1 /* node offset */, 3.4, true /* is null */);
    }

    void assertUpdatedAgeAndEyeSightPropertiesForNodes0And1(Transaction* transaction) {
        readAndAssertAgePropertyNode(0 /* node offset */, transaction, 70, false /* is not null */);
        readAndAssertAgePropertyNode(1 /* node offset */, transaction,
            1232532 /* this argument is ignored */, true /* is  null */);
        readAndAssertEyeSightPropertyNode(
            0 /* node offset */, transaction, 0.4, false /* is not null */);
        readAndAssertEyeSightPropertyNode(
            1 /* node offset */, transaction, -423.12 /* this is ignored */, true /* is null */);
    }

    void assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest() {
        assertOriginalAgeAndEyeSightPropertiesForNodes0And1(writeTrx.get());
        assertOriginalAgeAndEyeSightPropertiesForNodes0And1(readTrx.get());

        updateAgeAndEyeSightPropertiesForNodes0And1();

        assertUpdatedAgeAndEyeSightPropertiesForNodes0And1(writeTrx.get());
        assertOriginalAgeAndEyeSightPropertiesForNodes0And1(readTrx.get());
    }

    void testRecovery(bool committingTransaction) {
        assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest();
        getTransactionManager(*database)->commit(readTrx.get());
        commitAndCheckpointOrRollback(
            *database, writeTrx.get(), committingTransaction, true /* skip checkpointing */);
        readTrx = getTransactionManager(*database)->beginReadOnlyTransaction();
        // We next destroy and reconstruct the database to start up the system.
        // initWithoutLoadingGraph will construct a completely new databse, writeTrx, readTrx,
        // and personAgeColumn, and personEyeSightColumn classes. We could use completely
        // different variables here but we would need to duplicate this construction code.
        initWithoutLoadingGraph();
        if (committingTransaction) {
            assertUpdatedAgeAndEyeSightPropertiesForNodes0And1(writeTrx.get());
            assertUpdatedAgeAndEyeSightPropertiesForNodes0And1(readTrx.get());
        } else {
            assertOriginalAgeAndEyeSightPropertiesForNodes0And1(writeTrx.get());
            assertOriginalAgeAndEyeSightPropertiesForNodes0And1(readTrx.get());
        }
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

TEST_F(TransactionTests, Concurrent1Write1ReadTransactionInTheMiddleOfTransaction) {
    assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest();
}

TEST_F(TransactionTests, Concurrent1Write1ReadTransactionCommitAndCheckpoint) {
    assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest();

    // We need to commit the read transaction because  commitAndCheckpointOrRollback requires all
    // read transactions to leave the system.
    getTransactionManager(*database)->commit(readTrx.get());
    commitAndCheckpointOrRollback(*database, writeTrx.get(), true /* isCommit */);

    // At this point, the write transaction no longer is valid and is not registered in the
    // TransactionManager, so in normal operation, we would need to create new transactions.
    // Although this is not needed, we still do it. This is not needed because this test directly
    // accesses the columns with a transaction and the storage_structures assume that the given
    // transaction is active.
    writeTrx = getTransactionManager(*database)->beginWriteTransaction();
    readTrx = getTransactionManager(*database)->beginReadOnlyTransaction();

    assertUpdatedAgeAndEyeSightPropertiesForNodes0And1(writeTrx.get());
    assertUpdatedAgeAndEyeSightPropertiesForNodes0And1(readTrx.get());
}

// TODO(Semih): Later when we make these tests go through connection.cpp or write new tests that
// test the same functionality but in an end-to-end manner, check that the WAL is cleared and the
// active write transaction is removed from the transaction manager etc (e.g., that you can start a
// new  transaction).
TEST_F(TransactionTests, OpenReadOnlyTransactionTriggersTimeoutErrorForWriteTransaction) {
    // Note that TransactionTests starts 1 read and 1 write transaction by default.
    getTransactionManager(*database)->setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(
        10000 /* 10ms */);
    updateAgeAndEyeSightPropertiesForNodes0And1();
    try {
        commitAndCheckpointOrRollback(*database, writeTrx.get(), true /* isCommit */);
        FAIL();
    } catch (TransactionManagerException e) {
    } catch (Exception& e) { FAIL(); }
    assertOriginalAgeAndEyeSightPropertiesForNodes0And1(readTrx.get());
}

TEST_F(TransactionTests, Concurrent1Write1ReadTransactionRollback) {
    assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest();

    commitAndCheckpointOrRollback(*database, writeTrx.get(), false /* rollback */);

    // See the comment inside Concurrent1Write1ReadTransactionCommitAndCheckpoint for why we create
    // these new transactions
    writeTrx = getTransactionManager(*database)->beginWriteTransaction();
    readTrx = getTransactionManager(*database)->beginReadOnlyTransaction();

    assertOriginalAgeAndEyeSightPropertiesForNodes0And1(writeTrx.get());
    assertOriginalAgeAndEyeSightPropertiesForNodes0And1(readTrx.get());
}

TEST_F(TransactionTests, RecoverFromCommittedTransaction) {
    testRecovery(true /* commit the transaction */);
}

TEST_F(TransactionTests, RecoverFromUncommittedTransaction) {
    testRecovery(false /* do not commit the transaction */);
}

TEST_F(TransactionTests, ExecuteWriteQueryInReadOnlyTrx) {
    conn->beginReadOnlyTransaction();
    auto result = conn->query("CREATE (p:person {ID: 20})");
    ASSERT_EQ(
        result->getErrorMessage(), "Can't execute a write query inside a read-only transaction.");
}
