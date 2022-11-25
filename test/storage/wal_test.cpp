#include "test_helper/test_helper.h"

using namespace kuzu::testing;
using namespace kuzu::storage;

class WALTests : public Test {

protected:
    void SetUp() override {
        FileUtils::createDir(TestHelper::getTmpTestDir());
        bufferManager =
            make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        wal = make_unique<WAL>(TestHelper::getTmpTestDir(), *bufferManager);
    }

    void TearDown() override { FileUtils::removeDir(TestHelper::getTmpTestDir()); }

    void addStructuredNodePropertyMainFilePageRecord(
        vector<uint64_t>& assignedPageIndices, uint64_t numRecordsToAdd) {
        for (int i = 0; i < numRecordsToAdd; ++i) {
            table_id_t tableID = 4;
            uint32_t propertyID = 89;
            uint64_t pageIdxInOriginalFile = 1455304;
            uint64_t pageIdxInWAL = wal->logPageUpdateRecord(
                StorageStructureID::newStructuredNodePropertyColumnID(tableID, propertyID),
                pageIdxInOriginalFile);
            assignedPageIndices.push_back(pageIdxInWAL);
        }
    }

    void readAndVerifyStructuredNodePropertyMainFilePageRecords(WALIterator* walIterator,
        vector<uint64_t>& assignedPageIndices, uint64_t startOff, uint64_t numRecordsToVerify) {
        for (int i = 0; i < numRecordsToVerify; ++i) {
            WALRecord expectedRecord = WALRecord::newPageUpdateRecord(
                StorageStructureID::newStructuredNodePropertyColumnID(4, 89), 1455304,
                assignedPageIndices[startOff + i]);
            ASSERT_TRUE(walIterator->hasNextRecord());
            WALRecord retVal;
            walIterator->getNextRecord(retVal);
            ASSERT_EQ(retVal, expectedRecord);
        }
    }

    void addStructuredNodePropertyOveflowFilePageRecord(
        vector<uint64_t>& assignedPageIndices, uint64_t numRecordsToAdd) {
        for (int i = 0; i < numRecordsToAdd; ++i) {
            table_id_t tableID = 0;
            uint32_t propertyID = 2463;
            uint64_t pageIdxInOriginalFile = 44436;
            uint64_t pageIdxInWAL = wal->logPageUpdateRecord(
                StorageStructureID::newStructuredNodePropertyColumnID(tableID, propertyID),
                pageIdxInOriginalFile);
            assignedPageIndices.push_back(pageIdxInWAL);
        }
    }

    void readAndVerifyStructuredNodePropertyOveflowFilePageRecord(WALIterator* walIterator,
        vector<uint64_t>& assignedPageIndices, uint64_t startOff, uint64_t numRecordsToVerify) {
        for (int i = 0; i < numRecordsToVerify; ++i) {
            WALRecord expectedRecord = WALRecord::newPageUpdateRecord(
                StorageStructureID::newStructuredNodePropertyColumnID(0, 2463), 44436,
                assignedPageIndices[startOff + i]);
            ASSERT_TRUE(walIterator->hasNextRecord());
            WALRecord retVal;
            walIterator->getNextRecord(retVal);
            ASSERT_EQ(retVal, expectedRecord);
        }
    }

    void addCommitRecord() { wal->logCommit(8023634 /* transaction ID */); }

    void readAndVerifyCommitRecord(WALIterator* walIterator) {
        WALRecord expectedRecord = WALRecord::newCommitRecord(8023634 /* transaction ID */);
        ASSERT_TRUE(walIterator->hasNextRecord());
        WALRecord retVal;
        walIterator->getNextRecord(retVal);
        ASSERT_EQ(retVal, expectedRecord);
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<WAL> wal;
};

TEST_F(WALTests, LogNewStructuredNodePropertyMainPageTest) {
    vector<uint64_t> assignedPageIndices;
    addStructuredNodePropertyMainFilePageRecord(assignedPageIndices, 1 /* numRecordsToAdd */);
    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyMainFilePageRecords(
        walIterator.get(), assignedPageIndices, 0, 1);
}

TEST_F(WALTests, LogNewStructuredNodePropertyOverlfowPageTest) {
    vector<uint64_t> assignedPageIndices;
    addStructuredNodePropertyOveflowFilePageRecord(assignedPageIndices, 1 /* numRecordsToAdd */);
    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyOveflowFilePageRecord(
        walIterator.get(), assignedPageIndices, 0, 1);
}

TEST_F(WALTests, LogCommitTest) {
    addCommitRecord();
    auto walIterator = wal->getIterator();
    readAndVerifyCommitRecord(walIterator.get());
}

TEST_F(WALTests, LogManyRecords) {
    uint64_t numStructuredNodePropertyMainFilePageRecords = 1000;
    vector<uint64_t> assignedPageIdxs;
    addStructuredNodePropertyMainFilePageRecord(
        assignedPageIdxs, numStructuredNodePropertyMainFilePageRecords);

    uint64_t numStructuredNodePropertyOverflowPageRecords = 7000;
    addStructuredNodePropertyOveflowFilePageRecord(
        assignedPageIdxs, numStructuredNodePropertyOverflowPageRecords);
    addCommitRecord();

    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyMainFilePageRecords(walIterator.get(), assignedPageIdxs,
        0 /* startOffset in assignedPageIdxs */, numStructuredNodePropertyMainFilePageRecords);
    readAndVerifyStructuredNodePropertyOveflowFilePageRecord(walIterator.get(), assignedPageIdxs,
        numStructuredNodePropertyMainFilePageRecords /* startOffset in assignedPageIdxs */,
        numStructuredNodePropertyOverflowPageRecords);
    readAndVerifyCommitRecord(walIterator.get());
    ASSERT_FALSE(walIterator->hasNextRecord());
}

TEST_F(WALTests, TestDeleteAndReopenWALFile) {
    uint64_t numStructuredNodePropertyMainFilePageRecords = 1500;
    vector<uint64_t> assignedPageIdxs;
    addStructuredNodePropertyMainFilePageRecord(
        assignedPageIdxs, numStructuredNodePropertyMainFilePageRecords);
    addCommitRecord();
    // Pin and unpin some pages
    bufferManager->pin(*wal->fileHandle, assignedPageIdxs[100]);
    bufferManager->pin(*wal->fileHandle, assignedPageIdxs[150]);
    bufferManager->unpin(*wal->fileHandle, assignedPageIdxs[100]);
    bufferManager->unpin(*wal->fileHandle, assignedPageIdxs[150]);
    // We test that delete and reopen truncates the data
    wal->clearWAL();

    assignedPageIdxs.clear();
    addStructuredNodePropertyMainFilePageRecord(
        assignedPageIdxs, numStructuredNodePropertyMainFilePageRecords);

    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyMainFilePageRecords(walIterator.get(), assignedPageIdxs,
        0 /* startOffset in assignedPageIdxs */, numStructuredNodePropertyMainFilePageRecords);
    ASSERT_FALSE(walIterator->hasNextRecord());
}

TEST_F(WALTests, TestOpeningExistingWAL) {
    uint64_t numStructuredNodePropertyMainFilePageRecords = 100;
    vector<uint64_t> assignedPageIdxs;
    addStructuredNodePropertyMainFilePageRecord(
        assignedPageIdxs, numStructuredNodePropertyMainFilePageRecords);
    wal.reset();
    wal = make_unique<WAL>(TestHelper::getTmpTestDir(), *bufferManager);

    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyMainFilePageRecords(walIterator.get(), assignedPageIdxs,
        0 /* startOffset in assignedPageIdxs */, numStructuredNodePropertyMainFilePageRecords);
    ASSERT_FALSE(walIterator->hasNextRecord());
}
