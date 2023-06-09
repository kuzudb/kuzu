#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::storage;

class WALTests : public EmptyDBTest {

protected:
    void SetUp() override {
        EmptyDBTest::SetUp();
        FileUtils::createDir(databasePath);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::WAL);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::STORAGE);
        bufferManager = std::make_unique<BufferManager>(
            BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        wal = make_unique<WAL>(databasePath, *bufferManager);
    }

    void TearDown() override {
        EmptyDBTest::TearDown();
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::WAL);
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::STORAGE);
    }

    void addStructuredNodePropertyMainFilePageRecord(
        std::vector<uint64_t>& assignedPageIndices, uint64_t numRecordsToAdd) {
        for (int i = 0; i < numRecordsToAdd; ++i) {
            table_id_t tableID = 4;
            uint32_t propertyID = 89;
            uint64_t pageIdxInOriginalFile = 1455304;
            uint64_t pageIdxInWAL = wal->logPageUpdateRecord(
                StorageStructureID::newNodePropertyColumnID(tableID, propertyID),
                pageIdxInOriginalFile);
            assignedPageIndices.push_back(pageIdxInWAL);
        }
    }

    void readAndVerifyStructuredNodePropertyMainFilePageRecords(WALIterator* walIterator,
        std::vector<uint64_t>& assignedPageIndices, uint64_t startOff,
        uint64_t numRecordsToVerify) {
        for (int i = 0; i < numRecordsToVerify; ++i) {
            WALRecord expectedRecord =
                WALRecord::newPageUpdateRecord(StorageStructureID::newNodePropertyColumnID(4, 89),
                    1455304, assignedPageIndices[startOff + i]);
            ASSERT_TRUE(walIterator->hasNextRecord());
            WALRecord retVal;
            walIterator->getNextRecord(retVal);
            ASSERT_EQ(retVal, expectedRecord);
        }
    }

    void addStructuredNodePropertyOveflowFilePageRecord(
        std::vector<uint64_t>& assignedPageIndices, uint64_t numRecordsToAdd) {
        for (int i = 0; i < numRecordsToAdd; ++i) {
            table_id_t tableID = 0;
            uint32_t propertyID = 2463;
            uint64_t pageIdxInOriginalFile = 44436;
            uint64_t pageIdxInWAL = wal->logPageUpdateRecord(
                StorageStructureID::newNodePropertyColumnID(tableID, propertyID),
                pageIdxInOriginalFile);
            assignedPageIndices.push_back(pageIdxInWAL);
        }
    }

    void readAndVerifyStructuredNodePropertyOveflowFilePageRecord(WALIterator* walIterator,
        std::vector<uint64_t>& assignedPageIndices, uint64_t startOff,
        uint64_t numRecordsToVerify) {
        for (int i = 0; i < numRecordsToVerify; ++i) {
            WALRecord expectedRecord =
                WALRecord::newPageUpdateRecord(StorageStructureID::newNodePropertyColumnID(0, 2463),
                    44436, assignedPageIndices[startOff + i]);
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
    std::unique_ptr<BufferManager> bufferManager;
    std::unique_ptr<WAL> wal;
};

TEST_F(WALTests, LogNewStructuredNodePropertyMainPageTest) {
    std::vector<uint64_t> assignedPageIndices;
    addStructuredNodePropertyMainFilePageRecord(assignedPageIndices, 1 /* numRecordsToAdd */);
    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyMainFilePageRecords(
        walIterator.get(), assignedPageIndices, 0, 1);
}

TEST_F(WALTests, LogNewStructuredNodePropertyOverlfowPageTest) {
    std::vector<uint64_t> assignedPageIndices;
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
    std::vector<uint64_t> assignedPageIdxs;
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
    std::vector<uint64_t> assignedPageIdxs;
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
    std::vector<uint64_t> assignedPageIdxs;
    addStructuredNodePropertyMainFilePageRecord(
        assignedPageIdxs, numStructuredNodePropertyMainFilePageRecords);
    wal.reset();
    wal = make_unique<WAL>(databasePath, *bufferManager);

    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyMainFilePageRecords(walIterator.get(), assignedPageIdxs,
        0 /* startOffset in assignedPageIdxs */, numStructuredNodePropertyMainFilePageRecords);
    ASSERT_FALSE(walIterator->hasNextRecord());
}
