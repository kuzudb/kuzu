#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;
using namespace graphflow::storage;

class WALTests : public Test {

protected:
    void SetUp() override {
        FileUtils::createDir(TestHelper::TEMP_TEST_DIR);
        wal = make_unique<WAL>(string(TestHelper::TEMP_TEST_DIR) + "waltest.wal");
    }

    void TearDown() override { FileUtils::removeDir(TestHelper::TEMP_TEST_DIR); }

    void addStructuredNodePropertyPageRecord(
        vector<uint64_t>& assignedPageIndices, uint64_t numRecordsToAdd) {
        for (int i = 0; i < numRecordsToAdd; ++i) {
            label_t nodeLabel = 4;
            uint32_t propertyID = 89;
            uint64_t pageIdxInOriginalFile = 1455304;
            uint64_t pageIdxInWAL = wal->logStructuredNodePropertyPageRecord(
                nodeLabel, propertyID, pageIdxInOriginalFile);
            assignedPageIndices.push_back(pageIdxInWAL);
        }
    }

    void readAndVerifyStructuredNodePropertyPageRecords(WALIterator* walIterator,
        vector<uint64_t>& assignedPageIndices, uint64_t startOff, uint64_t numRecordsToVerify) {
        for (int i = 0; i < numRecordsToVerify; ++i) {
            WALRecord expectedRecord = WALRecord::newStructuredNodePropertyPageUpdateRecord(
                4, 89, 1455304, assignedPageIndices[startOff + i]);
            ASSERT_TRUE(walIterator->hasNextRecord());
            WALRecord retVal;
            walIterator->getNextRecord(retVal);
            ASSERT_EQ(retVal, expectedRecord);
        }
    }

    void addStructuredAdjColumnPropertyPageRecord(
        vector<uint64_t>& assignedPageIndices, uint64_t numRecordsToAdd) {
        for (int i = 0; i < numRecordsToAdd; ++i) {
            label_t nodeLabel = 0;
            label_t relLabel = 128;
            uint32_t propertyID = 2463;
            uint64_t pageIdxInOriginalFile = 44436;
            uint64_t pageIdxInWAL = wal->logStructuredAdjColumnPropertyPage(
                nodeLabel, relLabel, propertyID, pageIdxInOriginalFile);
            assignedPageIndices.push_back(pageIdxInWAL);
        }
    }

    void readAndVerifyStructuredAdjColumnPropertyPageRecords(WALIterator* walIterator,
        vector<uint64_t>& assignedPageIndices, uint64_t startOff, uint64_t numRecordsToVerify) {
        for (int i = 0; i < numRecordsToVerify; ++i) {
            WALRecord expectedRecord = WALRecord::newStructuredAdjColumnPropertyPageUpdateRecord(
                0, 128, 2463, 44436, assignedPageIndices[startOff + i]);
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
    unique_ptr<WAL> wal;
};

TEST_F(WALTests, LogNewStructuredNodePropertyPageTest) {
    vector<uint64_t> assignedPageIndices;
    addStructuredNodePropertyPageRecord(assignedPageIndices, 1 /* numRecordsToAdd */);
    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyPageRecords(walIterator.get(), assignedPageIndices, 0, 1);
}

TEST_F(WALTests, LogStructuredAdjColumnPropertyPageTest) {
    vector<uint64_t> assignedPageIndices;
    addStructuredAdjColumnPropertyPageRecord(assignedPageIndices, 1 /* numRecordsToAdd */);
    auto walIterator = wal->getIterator();
    readAndVerifyStructuredAdjColumnPropertyPageRecords(
        walIterator.get(), assignedPageIndices, 0, 1);
}

TEST_F(WALTests, LogCommitTest) {
    addCommitRecord();
    auto walIterator = wal->getIterator();
    readAndVerifyCommitRecord(walIterator.get());
}

TEST_F(WALTests, LogManyRecords) {
    uint64_t numStructuredNodePropertyPageRecords = 1000;
    vector<uint64_t> assignedPageIdxs;
    addStructuredNodePropertyPageRecord(assignedPageIdxs, numStructuredNodePropertyPageRecords);

    uint64_t numStructuredAdjColumnPropertyPageRecords = 7000;
    addStructuredAdjColumnPropertyPageRecord(
        assignedPageIdxs, numStructuredAdjColumnPropertyPageRecords);
    addCommitRecord();

    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyPageRecords(walIterator.get(), assignedPageIdxs,
        0 /* startOffset in assignedPageIdxs */, numStructuredNodePropertyPageRecords);
    readAndVerifyStructuredAdjColumnPropertyPageRecords(walIterator.get(), assignedPageIdxs,
        numStructuredNodePropertyPageRecords /* startOffset in assignedPageIdxs */,
        numStructuredAdjColumnPropertyPageRecords);
    readAndVerifyCommitRecord(walIterator.get());
    ASSERT_FALSE(walIterator->hasNextRecord());
}

TEST_F(WALTests, TestDeleteAndReopenWALFile) {
    uint64_t numStructuredNodePropertyPageRecords = 1500;
    vector<uint64_t> assignedPageIdxs;
    addStructuredNodePropertyPageRecord(assignedPageIdxs, numStructuredNodePropertyPageRecords);
    addCommitRecord();
    auto bufferManager = make_unique<BufferManager>();
    // Pin and unpin some pages
    bufferManager->pin(*wal->fileHandle, assignedPageIdxs[100]);
    bufferManager->pin(*wal->fileHandle, assignedPageIdxs[150]);
    bufferManager->unpin(*wal->fileHandle, assignedPageIdxs[100]);
    bufferManager->unpin(*wal->fileHandle, assignedPageIdxs[150]);
    // We test that delete and reopen truncates the data
    wal->deleteAndReopenWALFile(bufferManager.get());

    assignedPageIdxs.clear();
    addStructuredNodePropertyPageRecord(assignedPageIdxs, numStructuredNodePropertyPageRecords);

    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyPageRecords(walIterator.get(), assignedPageIdxs,
        0 /* startOffset in assignedPageIdxs */, numStructuredNodePropertyPageRecords);
    ASSERT_FALSE(walIterator->hasNextRecord());
}

TEST_F(WALTests, TestOpeningExistingWAL) {
    uint64_t numStructuredNodePropertyPageRecords = 100;
    vector<uint64_t> assignedPageIdxs;
    addStructuredNodePropertyPageRecord(assignedPageIdxs, numStructuredNodePropertyPageRecords);
    wal.reset();
    wal = make_unique<WAL>(string(TestHelper::TEMP_TEST_DIR) + "waltest.wal");

    auto walIterator = wal->getIterator();
    readAndVerifyStructuredNodePropertyPageRecords(walIterator.get(), assignedPageIdxs,
        0 /* startOffset in assignedPageIdxs */, numStructuredNodePropertyPageRecords);
    ASSERT_FALSE(walIterator->hasNextRecord());
}
