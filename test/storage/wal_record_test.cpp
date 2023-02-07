#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/wal/wal_record.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using ::testing::Test;

class WALRecordTest : public Test {

protected:
    // We assume 4096 bytes is enough as space two write logs in the tests in these files. When
    // other tests are written and this assumption fails, this needs to be increased.
    void SetUp() override { bytes = std::make_unique<uint8_t[]>(4096); }

    void TearDown() override {}

public:
    std::unique_ptr<uint8_t[]> bytes;

public:
    void readBackWALRecordAndAssert(WALRecord& expectedWALRecord, uint64_t& offset) {
        uint64_t previousOffset = offset;
        WALRecord recordReadBack;
        WALRecord::constructWALRecordFromBytes(recordReadBack, bytes.get(), offset);
        // Test the offset was moved correctly
        ASSERT_EQ(offset, previousOffset + sizeof(WALRecord));
        ASSERT_EQ(recordReadBack, expectedWALRecord);
    }

    void writeExpectedWALRecordReadBackAndAssert(WALRecord& expectedWALRecord) {
        uint64_t offset = 0;
        expectedWALRecord.writeWALRecordToBytes(bytes.get(), offset);
        // Test the offset was moved correctly
        ASSERT_EQ(offset, sizeof(WALRecord));
        // Test we read back what we wrote
        offset = 0;
        readBackWALRecordAndAssert(expectedWALRecord, offset);
    }

    WALRecord constructExampleStructuredNodePropertyMainFilePageUpdateRecord() {
        table_id_t tableID = 4;
        uint32_t propertyID = 89;
        uint64_t pageIdxInOriginalFile = 1455304;
        uint64_t pageIdxInWAL = 3;
        return WALRecord::newPageUpdateRecord(
            StorageStructureID::newNodePropertyColumnID(tableID, propertyID), pageIdxInOriginalFile,
            pageIdxInWAL);
    }

    WALRecord constructExampleStructuredNodePropertyOverflowFilePageUpdateRecord() {
        table_id_t tableID = 0;
        uint32_t propertyID = 2463;
        uint64_t pageIdxInOriginalFile = 44436;
        uint64_t pageIdxInWAL = 1234;
        return WALRecord::newPageUpdateRecord(
            StorageStructureID::newNodePropertyColumnID(tableID, propertyID), pageIdxInOriginalFile,
            pageIdxInWAL);
    }

    WALRecord constructExampleCommitRecord() {
        uint64_t transactionID = 8023634;
        return WALRecord::newCommitRecord(transactionID);
    }
};

TEST_F(WALRecordTest, StructuredNodePropertyPageUpdateRecordTest) {
    WALRecord expectedWALRecord = constructExampleStructuredNodePropertyMainFilePageUpdateRecord();
    writeExpectedWALRecordReadBackAndAssert(expectedWALRecord);
}

TEST_F(WALRecordTest, StructuredAdjColumnPropertyPageUpdateRecordTest) {
    WALRecord expectedWALRecord =
        constructExampleStructuredNodePropertyOverflowFilePageUpdateRecord();
    writeExpectedWALRecordReadBackAndAssert(expectedWALRecord);
}

TEST_F(WALRecordTest, CommitRecordTest) {
    WALRecord expectedWALRecord = constructExampleCommitRecord();
    writeExpectedWALRecordReadBackAndAssert(expectedWALRecord);
}

TEST_F(WALRecordTest, MultipleRecordWritingTest) {
    WALRecord expectedWALRecord1 = constructExampleStructuredNodePropertyMainFilePageUpdateRecord();
    WALRecord expectedWALRecord2 =
        constructExampleStructuredNodePropertyOverflowFilePageUpdateRecord();
    WALRecord expectedWALRecord3 = constructExampleCommitRecord();

    uint64_t offset = 0;
    expectedWALRecord1.writeWALRecordToBytes(bytes.get(), offset);
    ASSERT_EQ(offset, sizeof(WALRecord));
    expectedWALRecord2.writeWALRecordToBytes(bytes.get(), offset);
    ASSERT_EQ(offset, 2 * sizeof(WALRecord));
    expectedWALRecord3.writeWALRecordToBytes(bytes.get(), offset);
    ASSERT_EQ(offset, 3 * sizeof(WALRecord));
    offset = 0;
    readBackWALRecordAndAssert(expectedWALRecord1, offset);
    readBackWALRecordAndAssert(expectedWALRecord2, offset);
    readBackWALRecordAndAssert(expectedWALRecord3, offset);
}
