#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

// TODO(Guodong): the current disk array update test relies on unstructured property which
// has been removed in the recent PRs.

// Tests the core update functionality of DiskArray and InMemDiskArray. Specifically tests that
// updates to DiskArrays happen transactionally. The tests use ListHeaders, which consists of a
// single DiskArray as a wrapper. This is required and we cannot (easily) write tests that directly
// update the disk array and then checkpoint manually, and check that we obtain the correct version.
// The updates we perform on the list do not correspond to a real update that would happen on the
// database.
class BaseDiskArrayUpdateTests : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        initWithoutLoadingGraph();
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        auto personTableID =
            getCatalog(*database)->getReadOnlyVersion()->getNodeTableIDFromName("person");
        personNodeTable = getStorageManager(*database)->getNodesStore().getNodeTable(personTableID);
        conn->beginWriteTransaction();
    }

    // This function is necessary to call to trigger the checkpoint/rollback mechanism of the
    // database: since we are using list_headers as a wrapper around the headerDA DiskArray, which
    // we use as our DiskArray in these tests, a call to set the property list of nodeOffset to
    // empty will trigger the following: the UnstructuredPropertyLists will have an "update"; then
    // this will trigger UnstructuredPropertyLists to add itself to
    // wal->addToUpdatedUnstructuredPropertyLists when prepareCommitOrRollbackIfNecessary called;
    // then the WALReplayer will call checkpoint/rollbackInMemory on this UnstructuredPropertyLists;
    // finally UnstructuredPropertyLists will call headerDA->checkpoint/rollbackInMemoryIfNecessary.
    void setNodeOffset0ToEmptyListToTriggerCheckpointOrRecoveryMechanism() {
        personNodeTable->getUnstrPropertyLists()->initEmptyPropertyLists(0);
    }

    void testBasicUpdate(bool isCommit, TransactionTestType transactionTestType) {
        setNodeOffset0ToEmptyListToTriggerCheckpointOrRecoveryMechanism();
        // This is a simple test to test where we change each element i in a disk array with
        // value i and the verify that we are able to do this transactionally.
        auto headersDA =
            personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();
        // Warning: We are explicitly assuming here that page_idx_t is uint32_t. If this changes,
        // this test might break.
        for (uint32_t nodeOffset = 0; nodeOffset <= 999; ++nodeOffset) {
            headersDA->update(nodeOffset, nodeOffset);
        }

        // We first test that read and write trxs can read different values from a disk array.
        for (uint32_t nodeOffset = 0; nodeOffset <= 999; ++nodeOffset) {
            ASSERT_EQ(0, headersDA->get(nodeOffset, TransactionType::READ_ONLY));
            ASSERT_EQ(nodeOffset, headersDA->get(nodeOffset, TransactionType::WRITE));
        }

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        headersDA = personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();
        if (isCommit) {
            for (node_offset_t nodeOffset = 0; nodeOffset <= 999; ++nodeOffset) {
                ASSERT_EQ(nodeOffset, headersDA->get(nodeOffset, TransactionType::READ_ONLY));
                ASSERT_EQ(nodeOffset, headersDA->get(nodeOffset, TransactionType::WRITE));
            }
        } else {
            for (node_offset_t nodeOffset = 0; nodeOffset <= 999; ++nodeOffset) {
                ASSERT_EQ(0, headersDA->get(nodeOffset, TransactionType::READ_ONLY));
                ASSERT_EQ(0, headersDA->get(nodeOffset, TransactionType::WRITE));
            }
        }
    }

    void testBasicPushBack(bool isCommit, TransactionTestType transactionTestType) {
        setNodeOffset0ToEmptyListToTriggerCheckpointOrRecoveryMechanism();
        auto headersDA =
            personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();
        headersDA->pushBack(1234);
        ASSERT_EQ(1000, headersDA->getNumElements(TransactionType::READ_ONLY));
        try {
            headersDA->get(1000, TransactionType::READ_ONLY);
            FAIL();
        } catch (exception& e) {}
        ASSERT_EQ(1001, headersDA->getNumElements(TransactionType::WRITE));
        ASSERT_EQ(1234, headersDA->get(1000, TransactionType::WRITE));
        try {
            headersDA->get(1001, TransactionType::WRITE);
            FAIL();
        } catch (exception& e) {}

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);

        headersDA = personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();
        if (isCommit) {
            ASSERT_EQ(1001, headersDA->getNumElements(TransactionType::READ_ONLY));
            ASSERT_EQ(1001, headersDA->getNumElements(TransactionType::WRITE));
            ASSERT_EQ(1234, headersDA->get(1000, TransactionType::READ_ONLY));
            ASSERT_EQ(1234, headersDA->get(1000, TransactionType::WRITE));
        } else {
            ASSERT_EQ(1000, headersDA->getNumElements(TransactionType::READ_ONLY));
            ASSERT_EQ(1000, headersDA->getNumElements(TransactionType::WRITE));
        }
    }

    void testPushBack(
        uint64_t sizeAfterPushing, bool isCommit, TransactionTestType transactionTestType) {
        setNodeOffset0ToEmptyListToTriggerCheckpointOrRecoveryMechanism();
        auto headersDA =
            personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();
        auto oldNumPages = headersDA->getFileHandle()->getNumPages();
        uint64_t prevNumElements = headersDA->getNumElements(TransactionType::READ_ONLY);
        for (int arrayOff = prevNumElements; arrayOff < sizeAfterPushing; ++arrayOff) {
            headersDA->pushBack(arrayOff);
        }
        ASSERT_EQ(prevNumElements, headersDA->getNumElements(TransactionType::READ_ONLY));
        ASSERT_EQ(sizeAfterPushing, headersDA->getNumElements(TransactionType::WRITE));
        for (int arrayOff = prevNumElements; arrayOff < sizeAfterPushing; ++arrayOff) {
            ASSERT_EQ(arrayOff, headersDA->get(arrayOff, TransactionType::WRITE));
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        headersDA = personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();

        if (isCommit) {
            ASSERT_EQ(sizeAfterPushing, headersDA->getNumElements(TransactionType::READ_ONLY));
            ASSERT_EQ(sizeAfterPushing, headersDA->getNumElements(TransactionType::WRITE));
            for (int arrayOff = prevNumElements; arrayOff < sizeAfterPushing; ++arrayOff) {
                ASSERT_EQ(arrayOff, headersDA->get(arrayOff, TransactionType::READ_ONLY));
                ASSERT_EQ(arrayOff, headersDA->get(arrayOff, TransactionType::WRITE));
            }
        } else {
            ASSERT_EQ(oldNumPages, headersDA->getFileHandle()->getNumPages());
            ASSERT_EQ(prevNumElements, headersDA->getNumElements(TransactionType::READ_ONLY));
            ASSERT_EQ(prevNumElements, headersDA->getNumElements(TransactionType::WRITE));
        }
    }

    void testTwoPushBackBatches(bool isCommit, TransactionTestType transactionTestType,
        uint64_t sizeAfterPushingFirstPhase) {
        setNodeOffset0ToEmptyListToTriggerCheckpointOrRecoveryMechanism();
        uint64_t sizeAfterPushing = sizeAfterPushingFirstPhase;
        testPushBack(sizeAfterPushing, isCommit, transactionTestType);
        if (isCommit) {
            // If we have committed. We do another test and push back some more data (1 element
            // to be exact) and test that we can get read/write visibility correctly.
            sizeAfterPushing++;
            testPushBack(sizeAfterPushing, isCommit, transactionTestType);
        }
    }

public:
    NodeTable* personNodeTable;
};

class DiskArrayUpdateTests : public BaseDiskArrayUpdateTests {

public:
    string getInputDir() override { return "dataset/non-empty-disk-array-db/"; }
};

class DiskArrayUpdateEmptyDBTests : public BaseDiskArrayUpdateTests {

public:
    string getInputDir() override { return "dataset/empty-db/"; }
};

TEST_F(DiskArrayUpdateTests, BasicUpdateTestCommitNormalExecution) {
    testBasicUpdate(true /* is commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DiskArrayUpdateTests, BasicUpdateTestCommitRecovery) {
    testBasicUpdate(true /* is commit */, TransactionTestType::RECOVERY);
}

TEST_F(DiskArrayUpdateTests, BasicUpdateTestRollbackNormalExecution) {
    testBasicUpdate(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DiskArrayUpdateTests, BasicUpdateTestRollbackRecovery) {
    testBasicUpdate(false /* is rollback */, TransactionTestType::RECOVERY);
}

TEST_F(DiskArrayUpdateTests, BasicPushBackTestCommitNormalExecution) {
    testBasicPushBack(true /* is commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DiskArrayUpdateTests, BasicPushBackTestCommitRecovery) {
    testBasicPushBack(true /* is commit */, TransactionTestType::RECOVERY);
}

TEST_F(DiskArrayUpdateTests, BasicPushBackTestRollbackNormalExecution) {
    testBasicPushBack(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DiskArrayUpdateTests, BasicPushBackTestRollbackRecovery) {
    testBasicPushBack(false /* is rollback */, TransactionTestType::RECOVERY);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewAPTestCommitNormalExecution) {
    testPushBack(1025, true /* is commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewAPTestCommitRecovery) {
    testPushBack(1025, true /* is commit */, TransactionTestType::RECOVERY);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewAPTestRollbackNormalExecution) {
    testPushBack(1025, false /* is rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewAPTestRollbackRecovery) {
    testPushBack(1025, false /* is rollback */, TransactionTestType::RECOVERY);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewPIPTestCommitNormalExecution) {
    // 1023*1024 + 1 requires adding a new PIP in the first batch.
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::NORMAL_EXECUTION,
        1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewPIPTestCommitRecovery) {
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::RECOVERY,
        1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewPIPTestRollbackNormalExecution) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION,
        1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddNewPIPTestRollbackRecovery) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::RECOVERY,
        1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillOnePIPTestCommitNormalExecution) {
    // 1023*1024 fills an entire PIP in the first batch.
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::NORMAL_EXECUTION,
        1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillOnePIPTestCommitRecovery) {
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::RECOVERY,
        1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillOnePIPTestRollbackNormalExecution) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION,
        1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillOnePIPTestRollbackRecovery) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::RECOVERY,
        1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddTwoNewPIPsTestCommitNormalExecution) {
    // 2*1023*1024 + 1 requires adding two new PIPs in the first batch.
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::NORMAL_EXECUTION,
        2 * 1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddTwoNewPIPsTestCommitRecovery) {
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::RECOVERY,
        2 * 1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddTwoNewPIPsTestRollbackNormalExecution) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION,
        2 * 1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToAddTwoNewPIPsTestRollbackRecovery) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::RECOVERY,
        2 * 1023 * 1024 + 1 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillTwoPIPsTestCommitNormalExecution) {
    // 2*1023*1024 fills an entire PIP in the first batch.
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::NORMAL_EXECUTION,
        2 * 1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillTwoPIPsTestCommitRecovery) {
    testTwoPushBackBatches(true /* is commit */, TransactionTestType::RECOVERY,
        2 * 1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillTwoPIPsTestRollbackNormalExecution) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION,
        2 * 1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateTests, PushBackEnoughToFillTwoPIPsTestRollbackRecovery) {
    testTwoPushBackBatches(false /* is rollback */, TransactionTestType::RECOVERY,
        2 * 1023 * 1024 /* size after first batch */);
}

TEST_F(DiskArrayUpdateEmptyDBTests, EmptyDiskArrayUpdatesTest) {
    auto headers = personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();
    ASSERT_EQ(0, headers->getNumElements(TransactionType::READ_ONLY));
    ASSERT_EQ(0, headers->getNumElements(TransactionType::WRITE));

    // First test that we cannot update to 0th location, which doesn't exist.
    try {
        headers->update(0, 1234);
        FAIL();
    } catch (exception& e) {}
    setNodeOffset0ToEmptyListToTriggerCheckpointOrRecoveryMechanism();
    // In an ideal test, we should test that the WRITE and READ_ONLY transactions read different
    // values (e.g., WRITE sees a numElements of 1 and READ_ONLY sees 0). However we cannot do that
    // test here because unstructured property lists do not update the header until during
    // committing. Instead currently it accumulates all updates in its local
    // UnstructuredListUpdateStore. So even if we read the header's size for WRITE transaction, we
    // would read 0 before committing. So the only thing we can test is that after the commit, both
    // READ and WRITE sees a header with size 1 and headers[0] = 0 (which is the header empty lists
    // get).
    commitOrRollbackConnectionAndInitDBIfNecessary(
        true /* is commit */, TransactionTestType::NORMAL_EXECUTION);
    headers = personNodeTable->getUnstrPropertyLists()->getHeaders()->headersDiskArray.get();
    ASSERT_EQ(1, headers->getNumElements(TransactionType::READ_ONLY));
    ASSERT_EQ(1, headers->getNumElements(TransactionType::WRITE));
    ASSERT_EQ(0, headers->get(0, TransactionType::READ_ONLY));
    ASSERT_EQ(0, headers->get(0, TransactionType::WRITE));
}
