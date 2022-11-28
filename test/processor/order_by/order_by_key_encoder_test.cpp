#include <vector>

#include "common/configs.h"
#include "common/data_chunk/data_chunk.h"
#include "gtest/gtest.h"
#include "processor/operator/order_by/order_by_key_encoder.h"

using ::testing::Test;
using namespace kuzu::processor;
using namespace std;

class OrderByKeyEncoderTest : public Test {

public:
    void SetUp() override {
        bufferManager =
            make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
    }

    void checkTupleIdxAndFactorizedTableIdx(uint64_t expectedBlockOffset, uint8_t*& keyBlockPtr) {
        ASSERT_EQ(OrderByKeyEncoder::getEncodedFTBlockIdx(keyBlockPtr), 0);
        ASSERT_EQ(OrderByKeyEncoder::getEncodedFTBlockOffset(keyBlockPtr), expectedBlockOffset);
        ASSERT_EQ(OrderByKeyEncoder::getEncodedFTIdx(keyBlockPtr), ftIdx);
        keyBlockPtr += 8;
    }

    // This method can only be used to check the null flag for a not null value.
    // We should call checkNullVal directly to check a null value.
    inline void checkNonNullFlag(uint8_t*& keyBlockPtr, bool isAsc) {
        ASSERT_EQ(*(keyBlockPtr++), isAsc ? 0x00 : 0xFF);
    }

    // If the col is in asc order, the encoding string is:
    // 0xFF(null flag) + 0xFF...FF(padding)
    // if the col is in desc order, the encoding string is:
    // 0x00(null flag) + 0x00...00(padding)
    inline void checkNullVal(uint8_t*& keyBlockPtr, DataTypeID dataTypeID, bool isAsc) {
        for (auto i = 0u; i < OrderByKeyEncoder::getEncodingSize(DataType(dataTypeID)); i++) {
            ASSERT_EQ(*(keyBlockPtr++), isAsc ? 0xFF : 0x00);
        }
    }

    // This function generates a ValueVector of int64 tuples that are all 5.
    vector<shared_ptr<ValueVector>> getInt64TestValueVector(
        const uint64_t numOfElementsPerCol, const uint64_t numOfOrderByCols, bool flatCol) {
        shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(numOfOrderByCols);
        dataChunk->state->selVector->selectedSize = numOfElementsPerCol;
        vector<shared_ptr<ValueVector>> valueVectors;
        for (auto i = 0u; i < numOfOrderByCols; i++) {
            shared_ptr<ValueVector> valueVector =
                make_shared<ValueVector>(INT64, memoryManager.get());
            for (auto j = 0u; j < numOfElementsPerCol; j++) {
                valueVector->setValue(j, (int64_t)5);
            }
            dataChunk->insert(i, valueVector);
            valueVector->state->currIdx = flatCol ? 0 : -1;
            valueVectors.emplace_back(valueVector);
        }
        return valueVectors;
    }

    // This function assumes that all columns have datatype: INT64, and each tuple is 5.
    void checkKeyBlockForInt64TestValueVector(vector<shared_ptr<ValueVector>>& valueVectors,
        vector<shared_ptr<DataBlock>>& keyBlocks, uint64_t numOfElements, vector<bool>& isAscOrder,
        uint64_t numTuplesPerBlock) {
        for (auto i = 0u; i < keyBlocks.size(); i++) {
            auto numOfElementsToCheck = min(numOfElements, numTuplesPerBlock);
            numOfElements -= numOfElementsToCheck;
            auto keyBlockPtr = keyBlocks[i]->getData();
            for (auto j = 0u; j < numOfElementsToCheck; j++) {
                for (auto k = 0u; k < valueVectors.size(); k++) {
                    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
                    if (isAscOrder[0]) {
                        // Check encoding for: NULL FLAG(0x00) + 5=0x8000000000000005(big endian).
                        ASSERT_EQ(*(keyBlockPtr++), 0x80);
                        for (auto k = 0u; k < 6; k++) {
                            ASSERT_EQ(*(keyBlockPtr++), 0x00);
                        }
                        ASSERT_EQ(*(keyBlockPtr++), 0x05);
                    } else {
                        // Check encoding for: NULL FLAG(0xFF) + 5=0x7FFFFFFFFFFFFFFFFA(big endian).
                        // Note: we need to flip all bits since this column is in descending order.
                        ASSERT_EQ(*(keyBlockPtr++), 0x7F);
                        for (auto k = 0u; k < 6; k++) {
                            ASSERT_EQ(*(keyBlockPtr++), 0xFF);
                        }
                        ASSERT_EQ(*(keyBlockPtr++), 0xFA);
                    }
                }
                checkTupleIdxAndFactorizedTableIdx(i * numTuplesPerBlock + j, keyBlockPtr);
            }
        }
    }

    void singleOrderByColMultiBlockTest(bool isFlat) {
        uint64_t numOfElements = 2000;
        auto valueVectors = getInt64TestValueVector(numOfElements, 1, isFlat);
        auto isAscOrder = vector<bool>(1, false);
        auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(),
            ftIdx, numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
        if (isFlat) {
            for (auto i = 0u; i < numOfElements; i++) {
                orderByKeyEncoder.encodeKeys();
                valueVectors[0]->state->currIdx++;
            }
        } else {
            orderByKeyEncoder.encodeKeys();
        }
        checkKeyBlockForInt64TestValueVector(valueVectors, orderByKeyEncoder.getKeyBlocks(),
            numOfElements, isAscOrder, orderByKeyEncoder.getMaxNumTuplesPerBlock());
    }

    static inline void checkLongStrFlag(uint8_t*& keyBlockPtr, bool isAscOrder, bool isLongString) {
        ASSERT_EQ(*(keyBlockPtr++), isAscOrder == isLongString ? UINT8_MAX : 0);
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    const uint32_t ftIdx = 14;
    const uint32_t numTuplesPerBlockInFT = LARGE_PAGE_SIZE / 8;
};

TEST_F(OrderByKeyEncoderTest, singleOrderByColInt64UnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = 6;
    shared_ptr<ValueVector> int64ValueVector = make_shared<ValueVector>(INT64, memoryManager.get());
    int64ValueVector->setValue(0, (int64_t)73); // positive number
    int64ValueVector->setNull(1, true);
    int64ValueVector->setValue(2, (int64_t)-132);  // negative 1 byte number
    int64ValueVector->setValue(3, (int64_t)-5242); // negative 2 bytes number
    int64ValueVector->setValue(4, (int64_t)INT64_MAX);
    int64ValueVector->setValue(5, (int64_t)INT64_MIN);
    dataChunk->insert(0, int64ValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(int64ValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) + 73=0x8000000000000049(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0x00);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x49);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, INT64, isAscOrder[0]);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -132=0x7FFFFFFFFFFFFF7C(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x7C);
    checkTupleIdxAndFactorizedTableIdx(2, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -5242=0x7FFFFFFFFFFFEB86(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 5; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0xEB);
    ASSERT_EQ(*(keyBlockPtr++), 0x86);
    checkTupleIdxAndFactorizedTableIdx(3, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + INT64_MAX=0xFFFFFFFFFFFFFFFF(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    for (auto i = 0U; i < 8; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    checkTupleIdxAndFactorizedTableIdx(4, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + INT64_MIN=0x0000000000000000(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    for (auto i = 0u; i < 8; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0x00);
    }
    checkTupleIdxAndFactorizedTableIdx(5, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColInt64UnflatWithFilterTest) {
    // This test is used to test whether the orderByKeyEncoder correctly encodes the filtered
    // valueVector.
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    shared_ptr<ValueVector> int64ValueVector = make_shared<ValueVector>(INT64, memoryManager.get());
    int64ValueVector->setValue(0, (int64_t)73);
    int64ValueVector->setValue(1, (int64_t)-52);
    int64ValueVector->setValue(2, (int64_t)-132);
    dataChunk->insert(0, int64ValueVector);
    // Only the first and the third value is selected, so the encoder should
    // not encode the second value.
    int64ValueVector->state->selVector->resetSelectorToValuePosBuffer();
    int64ValueVector->state->selVector->selectedPositions[0] = 0;
    int64ValueVector->state->selVector->selectedPositions[1] = 2;
    int64ValueVector->state->selVector->selectedSize = 2;
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(int64ValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) + 73=0x8000000000000049(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0x00);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x49);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -132=0x7FFFFFFFFFFFFF7C(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x7C);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColBoolUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = 3;
    shared_ptr<ValueVector> boolValueVector = make_shared<ValueVector>(BOOL, memoryManager.get());
    boolValueVector->setValue(0, true);
    boolValueVector->setValue(1, false);
    boolValueVector->setNull(2, true);
    dataChunk->insert(0, boolValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(boolValueVector);
    auto isAscOrder = vector<bool>(1, false);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) + true=0xFE(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xFE);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + false=0xFF(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);

    checkNullVal(keyBlockPtr, BOOL, isAscOrder[0]);
    checkTupleIdxAndFactorizedTableIdx(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColDateUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = 3;
    shared_ptr<ValueVector> dateValueVector = make_shared<ValueVector>(DATE, memoryManager.get());
    dateValueVector->setValue(
        0, Date::FromCString("2035-07-04", strlen("2035-07-04"))); // date after 1970-01-01
    dateValueVector->setNull(1, true);
    dateValueVector->setValue(
        2, Date::FromCString("1949-10-01", strlen("1949-10-01"))); // date before 1970-01-01
    dataChunk->insert(0, dateValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(dateValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) + "2035-07-04"=0x80005D75(23925 days in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x5D);
    ASSERT_EQ(*(keyBlockPtr++), 0x75);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, DATE, isAscOrder[0]);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + "1949-10-01"=0x7FFFE31B(-7397 days in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    ASSERT_EQ(*(keyBlockPtr++), 0xE3);
    ASSERT_EQ(*(keyBlockPtr++), 0x1B);
    checkTupleIdxAndFactorizedTableIdx(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColTimestampUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = 3;
    shared_ptr<ValueVector> timestampValueVector =
        make_shared<ValueVector>(TIMESTAMP, memoryManager.get());
    // timestamp before 1970-01-01
    timestampValueVector->setValue(
        0, Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")));
    timestampValueVector->setNull(1, true);
    // timestamp after 1970-01-01
    timestampValueVector->setValue(
        2, Timestamp::FromCString("2035-07-01 11:14:33", strlen("2035-07-01 11:14:33")));
    dataChunk->insert(0, timestampValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(timestampValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) + "1962-04-07 11:12:35.123"=0x7FFF21F7F9D08F38
    // (-244126044877000 micros in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    ASSERT_EQ(*(keyBlockPtr++), 0x21);
    ASSERT_EQ(*(keyBlockPtr++), 0xF7);
    ASSERT_EQ(*(keyBlockPtr++), 0xF9);
    ASSERT_EQ(*(keyBlockPtr++), 0xD0);
    ASSERT_EQ(*(keyBlockPtr++), 0x8F);
    ASSERT_EQ(*(keyBlockPtr++), 0x38);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, TIMESTAMP, isAscOrder[0]);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + "2035-07-01 11:14:33"=0x800757D5F429B840
    // (2066901273000000 micros in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x07);
    ASSERT_EQ(*(keyBlockPtr++), 0x57);
    ASSERT_EQ(*(keyBlockPtr++), 0xD5);
    ASSERT_EQ(*(keyBlockPtr++), 0xF4);
    ASSERT_EQ(*(keyBlockPtr++), 0x29);
    ASSERT_EQ(*(keyBlockPtr++), 0xB8);
    ASSERT_EQ(*(keyBlockPtr++), 0x40);
    checkTupleIdxAndFactorizedTableIdx(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColIntervalUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = 2;
    shared_ptr<ValueVector> intervalValueVector =
        make_shared<ValueVector>(INTERVAL, memoryManager.get());
    intervalValueVector->setValue(
        0, Interval::FromCString("18 hours 55 days 13 years 8 milliseconds 3 months",
               strlen("18 hours 55 days 13 years 8 milliseconds 3 months")));
    intervalValueVector->setNull(1, true);
    dataChunk->insert(0, intervalValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(intervalValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) +  "18 hours 55 days 13 years 8 milliseconds 3 months"
    // = NULL FLAG(0x00) + 160 months(0x800000A0) + 25 days(0x80000019)
    // + 64800008000 micros(0x8000000F1661A740).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    // Check for months: 160 (0x800000A0 in big endian).
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0xA0);
    // Check for days: 25 (0x80000019 in big endian).
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x19);
    // Check for micros: 64800008000 (0x8000000F1661A740 in big endian).
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x0F);
    ASSERT_EQ(*(keyBlockPtr++), 0x16);
    ASSERT_EQ(*(keyBlockPtr++), 0x61);
    ASSERT_EQ(*(keyBlockPtr++), 0xA7);
    ASSERT_EQ(*(keyBlockPtr++), 0x40);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, INTERVAL, isAscOrder[0]);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColStringUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = 4;
    shared_ptr<ValueVector> stringValueVector =
        make_shared<ValueVector>(STRING, memoryManager.get());
    stringValueVector->setValue<string>(0, "short str"); // short string
    stringValueVector->setNull(1, true);
    stringValueVector->setValue<string>(
        2, "commonprefix string1"); // long string(encoding: commonprefix)
    stringValueVector->setValue<string>(
        3, "commonprefix string2"); // long string(encoding: commonprefix)
    dataChunk->insert(0, stringValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(stringValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) + "short str".
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 's');
    ASSERT_EQ(*(keyBlockPtr++), 'h');
    ASSERT_EQ(*(keyBlockPtr++), 'o');
    ASSERT_EQ(*(keyBlockPtr++), 'r');
    ASSERT_EQ(*(keyBlockPtr++), 't');
    ASSERT_EQ(*(keyBlockPtr++), ' ');
    ASSERT_EQ(*(keyBlockPtr++), 's');
    ASSERT_EQ(*(keyBlockPtr++), 't');
    ASSERT_EQ(*(keyBlockPtr++), 'r');
    ASSERT_EQ(*(keyBlockPtr++), '\0');
    ASSERT_EQ(*(keyBlockPtr++), '\0');
    ASSERT_EQ(*(keyBlockPtr++), '\0');
    checkLongStrFlag(keyBlockPtr, isAscOrder[0], false /* isLongStr */);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, STRING, isAscOrder[0]);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + "commonprefix string1".
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 'c');
    ASSERT_EQ(*(keyBlockPtr++), 'o');
    ASSERT_EQ(*(keyBlockPtr++), 'm');
    ASSERT_EQ(*(keyBlockPtr++), 'm');
    ASSERT_EQ(*(keyBlockPtr++), 'o');
    ASSERT_EQ(*(keyBlockPtr++), 'n');
    ASSERT_EQ(*(keyBlockPtr++), 'p');
    ASSERT_EQ(*(keyBlockPtr++), 'r');
    ASSERT_EQ(*(keyBlockPtr++), 'e');
    ASSERT_EQ(*(keyBlockPtr++), 'f');
    ASSERT_EQ(*(keyBlockPtr++), 'i');
    ASSERT_EQ(*(keyBlockPtr++), 'x');
    checkLongStrFlag(keyBlockPtr, isAscOrder[0], true /* isLongStr */);
    checkTupleIdxAndFactorizedTableIdx(2, keyBlockPtr);

    // Check encoding for val: NULL FLAG(0x00) + "commonprefix string2".
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 'c');
    ASSERT_EQ(*(keyBlockPtr++), 'o');
    ASSERT_EQ(*(keyBlockPtr++), 'm');
    ASSERT_EQ(*(keyBlockPtr++), 'm');
    ASSERT_EQ(*(keyBlockPtr++), 'o');
    ASSERT_EQ(*(keyBlockPtr++), 'n');
    ASSERT_EQ(*(keyBlockPtr++), 'p');
    ASSERT_EQ(*(keyBlockPtr++), 'r');
    ASSERT_EQ(*(keyBlockPtr++), 'e');
    ASSERT_EQ(*(keyBlockPtr++), 'f');
    ASSERT_EQ(*(keyBlockPtr++), 'i');
    ASSERT_EQ(*(keyBlockPtr++), 'x');
    checkLongStrFlag(keyBlockPtr, isAscOrder[0], true /* isLongStr */);
    checkTupleIdxAndFactorizedTableIdx(3, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColDoubleUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = 6;
    shared_ptr<ValueVector> doubleValueVector =
        make_shared<ValueVector>(DOUBLE, memoryManager.get());
    doubleValueVector->setValue(0, (double_t)3.452); // small positive number
    doubleValueVector->setNull(1, true);
    doubleValueVector->setValue(2, (double_t)-0.00031213);     // very small negative number
    doubleValueVector->setValue(3, (double_t)-5.42113);        // small negative number
    doubleValueVector->setValue(4, (double_t)92931312341415);  // large positive number
    doubleValueVector->setValue(5, (double_t)-31234142783434); // large negative number
    dataChunk->insert(0, doubleValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(doubleValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    // Check encoding for: NULL FLAG(0x00) + 3.452=0xC00B9DB22D0E5604(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xC0);
    ASSERT_EQ(*(keyBlockPtr++), 0x0B);
    ASSERT_EQ(*(keyBlockPtr++), 0x9D);
    ASSERT_EQ(*(keyBlockPtr++), 0xB2);
    ASSERT_EQ(*(keyBlockPtr++), 0x2D);
    ASSERT_EQ(*(keyBlockPtr++), 0x0E);
    ASSERT_EQ(*(keyBlockPtr++), 0x56);
    ASSERT_EQ(*(keyBlockPtr++), 0x04);
    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, INT64, isAscOrder[0]);
    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -0.00031213=0x40CB8B53DB9F4D8D(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x40);
    ASSERT_EQ(*(keyBlockPtr++), 0xCB);
    ASSERT_EQ(*(keyBlockPtr++), 0x8B);
    ASSERT_EQ(*(keyBlockPtr++), 0x53);
    ASSERT_EQ(*(keyBlockPtr++), 0xDB);
    ASSERT_EQ(*(keyBlockPtr++), 0x9F);
    ASSERT_EQ(*(keyBlockPtr++), 0x4D);
    ASSERT_EQ(*(keyBlockPtr++), 0x8D);
    checkTupleIdxAndFactorizedTableIdx(2, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -5.42113=0x3FEA50C34C1A8AC5(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x3F);
    ASSERT_EQ(*(keyBlockPtr++), 0xEA);
    ASSERT_EQ(*(keyBlockPtr++), 0x50);
    ASSERT_EQ(*(keyBlockPtr++), 0xC3);
    ASSERT_EQ(*(keyBlockPtr++), 0x4C);
    ASSERT_EQ(*(keyBlockPtr++), 0x1A);
    ASSERT_EQ(*(keyBlockPtr++), 0x8A);
    ASSERT_EQ(*(keyBlockPtr++), 0xC5);
    checkTupleIdxAndFactorizedTableIdx(3, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + 92931312341415=0xC2D52150771469C0(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xC2);
    ASSERT_EQ(*(keyBlockPtr++), 0xD5);
    ASSERT_EQ(*(keyBlockPtr++), 0x21);
    ASSERT_EQ(*(keyBlockPtr++), 0x50);
    ASSERT_EQ(*(keyBlockPtr++), 0x77);
    ASSERT_EQ(*(keyBlockPtr++), 0x14);
    ASSERT_EQ(*(keyBlockPtr++), 0x69);
    ASSERT_EQ(*(keyBlockPtr++), 0xC0);
    checkTupleIdxAndFactorizedTableIdx(4, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -31234142783434=0x3D4397BC03B835FF(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x3D);
    ASSERT_EQ(*(keyBlockPtr++), 0x43);
    ASSERT_EQ(*(keyBlockPtr++), 0x97);
    ASSERT_EQ(*(keyBlockPtr++), 0xBC);
    ASSERT_EQ(*(keyBlockPtr++), 0x03);
    ASSERT_EQ(*(keyBlockPtr++), 0xB8);
    ASSERT_EQ(*(keyBlockPtr++), 0x35);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    checkTupleIdxAndFactorizedTableIdx(5, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, largeNumBytesPerTupleErrorTest) {
    // If the numBytesPerTuple is larger than 4096 bytes, the encoder will raise an encoding
    // exception we need ((LARGE_PAGE_SIZE - 8) / 9  + 1 number of columns(with datatype INT) to
    // trigger that exception.
    auto numOfOrderByCols = (LARGE_PAGE_SIZE - 8) / 9 + 1;
    auto valueVectors = getInt64TestValueVector(1, numOfOrderByCols, true);
    auto isAscOrder = vector<bool>(numOfOrderByCols, true);
    try {
        auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(),
            ftIdx, numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
        FAIL();
    } catch (Exception& e) {
        ASSERT_STREQ(e.what(),
            StringUtils::string_format("Runtime exception: TupleSize(%d bytes) is larger than "
                                       "the LARGE_PAGE_SIZE(%d bytes)",
                9 * numOfOrderByCols + 8, LARGE_PAGE_SIZE)
                .c_str());
    } catch (exception& e) { FAIL(); }
}

TEST_F(OrderByKeyEncoderTest, singleTuplePerBlockTest) {
    uint32_t numOfOrderByCols = (LARGE_PAGE_SIZE - 8) / 9;
    uint32_t numOfElementsPerCol = 10;
    auto valueVectors = getInt64TestValueVector(numOfElementsPerCol, numOfOrderByCols, true);
    auto isAscOrder = vector<bool>(numOfOrderByCols, false);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    for (auto i = 0u; i < numOfElementsPerCol; i++) {
        orderByKeyEncoder.encodeKeys();
        valueVectors[0]->state->currIdx++;
    }
    auto& keyBlocks = orderByKeyEncoder.getKeyBlocks();
    checkKeyBlockForInt64TestValueVector(valueVectors, keyBlocks, numOfElementsPerCol, isAscOrder,
        orderByKeyEncoder.getMaxNumTuplesPerBlock());
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColMultiBlockUnflatTest) {
    singleOrderByColMultiBlockTest(false);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColMultiBlockFlatTest) {
    singleOrderByColMultiBlockTest(true);
}

TEST_F(OrderByKeyEncoderTest, multipleOrderByColSingleBlockTest) {
    vector<bool> isAscOrder = {true, false, true, true, true};
    auto intFlatValueVector = make_shared<ValueVector>(INT64, memoryManager.get());
    auto doubleFlatValueVector = make_shared<ValueVector>(DOUBLE, memoryManager.get());
    auto stringFlatValueVector = make_shared<ValueVector>(STRING, memoryManager.get());
    auto timestampFlatValueVector = make_shared<ValueVector>(TIMESTAMP, memoryManager.get());
    auto dateFlatValueVector = make_shared<ValueVector>(DATE, memoryManager.get());

    auto mockDataChunk = make_shared<DataChunk>(5);
    mockDataChunk->insert(0, intFlatValueVector);
    mockDataChunk->insert(1, doubleFlatValueVector);
    mockDataChunk->insert(2, stringFlatValueVector);
    mockDataChunk->insert(3, timestampFlatValueVector);
    mockDataChunk->insert(4, dateFlatValueVector);

    intFlatValueVector->state->currIdx = 0;
    doubleFlatValueVector->state->currIdx = 0;
    stringFlatValueVector->state->currIdx = 0;
    timestampFlatValueVector->state->currIdx = 0;
    dateFlatValueVector->state->currIdx = 0;

    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(intFlatValueVector);
    valueVectors.emplace_back(doubleFlatValueVector);
    valueVectors.emplace_back(stringFlatValueVector);
    valueVectors.emplace_back(timestampFlatValueVector);
    valueVectors.emplace_back(dateFlatValueVector);

    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->getData();

    intFlatValueVector->setValue(0, (int64_t)73);
    intFlatValueVector->setValue(1, (int64_t)-132);
    intFlatValueVector->setValue(2, (int64_t)-412414);
    doubleFlatValueVector->setValue(0, (double_t)53.421);
    doubleFlatValueVector->setValue(1, (double_t)-415.23);
    doubleFlatValueVector->setNull(2, true);
    stringFlatValueVector->setNull(0, true);
    stringFlatValueVector->setValue<string>(1, "this is a test string!!");
    stringFlatValueVector->setValue<string>(2, "short str");
    timestampFlatValueVector->setValue(
        0, Timestamp::FromCString("2008-08-08 20:20:20", strlen("2008-08-08 20:20:20")));
    timestampFlatValueVector->setValue(
        1, Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")));
    timestampFlatValueVector->setNull(2, true);
    dateFlatValueVector->setValue(0, Date::FromCString("1978-09-12", strlen("1978-09-12")));
    dateFlatValueVector->setValue(1, Date::FromCString("2035-07-04", strlen("2035-07-04")));
    dateFlatValueVector->setNull(2, true);
    for (auto i = 0u; i < 3; i++) {
        orderByKeyEncoder.encodeKeys();
        mockDataChunk->state->currIdx++;
    }

    // Check encoding for: NULL FLAG(0x00) + 73=0x8000000000000049(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0x00);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x49);

    // Check encoding for: NULL FLAG(0x00) + 53.421=0x3FB54A1CAC083126(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[1]);
    ASSERT_EQ(*(keyBlockPtr++), 0x3F);
    ASSERT_EQ(*(keyBlockPtr++), 0xB5);
    ASSERT_EQ(*(keyBlockPtr++), 0x4A);
    ASSERT_EQ(*(keyBlockPtr++), 0x1C);
    ASSERT_EQ(*(keyBlockPtr++), 0xAC);
    ASSERT_EQ(*(keyBlockPtr++), 0x08);
    ASSERT_EQ(*(keyBlockPtr++), 0x31);
    ASSERT_EQ(*(keyBlockPtr++), 0x26);

    checkNullVal(keyBlockPtr, STRING, isAscOrder[2]);

    // Check encoding for: NULL FLAG(0x00) + "2008-08-08 20:20:20"=0x800453F888DCA900
    // (1218226820000000 micros in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[3]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x04);
    ASSERT_EQ(*(keyBlockPtr++), 0x53);
    ASSERT_EQ(*(keyBlockPtr++), 0xF8);
    ASSERT_EQ(*(keyBlockPtr++), 0x88);
    ASSERT_EQ(*(keyBlockPtr++), 0xDC);
    ASSERT_EQ(*(keyBlockPtr++), 0xA9);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);

    // Check encoding for: NULL FLAG(0x00) + "1978-09-12"=0x80000C68(3176 days in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[4]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x0C);
    ASSERT_EQ(*(keyBlockPtr++), 0x68);

    checkTupleIdxAndFactorizedTableIdx(0, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -132=0x7FFFFFFFFFFFFF7C(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x7C);

    // Check encoding for: NULL FLAG(0x00) + -415.23=0xC079F3AE147AE148(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[1]);
    ASSERT_EQ(*(keyBlockPtr++), 0xC0);
    ASSERT_EQ(*(keyBlockPtr++), 0x79);
    ASSERT_EQ(*(keyBlockPtr++), 0xF3);
    ASSERT_EQ(*(keyBlockPtr++), 0xAE);
    ASSERT_EQ(*(keyBlockPtr++), 0x14);
    ASSERT_EQ(*(keyBlockPtr++), 0x7A);
    ASSERT_EQ(*(keyBlockPtr++), 0xE1);
    ASSERT_EQ(*(keyBlockPtr++), 0x48);

    // Check encoding for: "this is a test string!!".
    checkNonNullFlag(keyBlockPtr, isAscOrder[2]);
    ASSERT_EQ(*(keyBlockPtr++), 't');
    ASSERT_EQ(*(keyBlockPtr++), 'h');
    ASSERT_EQ(*(keyBlockPtr++), 'i');
    ASSERT_EQ(*(keyBlockPtr++), 's');
    ASSERT_EQ(*(keyBlockPtr++), ' ');
    ASSERT_EQ(*(keyBlockPtr++), 'i');
    ASSERT_EQ(*(keyBlockPtr++), 's');
    ASSERT_EQ(*(keyBlockPtr++), ' ');
    ASSERT_EQ(*(keyBlockPtr++), 'a');
    ASSERT_EQ(*(keyBlockPtr++), ' ');
    ASSERT_EQ(*(keyBlockPtr++), 't');
    ASSERT_EQ(*(keyBlockPtr++), 'e');
    checkLongStrFlag(keyBlockPtr, isAscOrder[2], true /* isLongStr */);

    // Check encoding for: NULL FLAG(0x00) + "1962-04-07 11:12:35.123"=0x7FFF21F7F9D08F38
    // (-244126044877000 micros in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[3]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    ASSERT_EQ(*(keyBlockPtr++), 0x21);
    ASSERT_EQ(*(keyBlockPtr++), 0xF7);
    ASSERT_EQ(*(keyBlockPtr++), 0xF9);
    ASSERT_EQ(*(keyBlockPtr++), 0xD0);
    ASSERT_EQ(*(keyBlockPtr++), 0x8F);
    ASSERT_EQ(*(keyBlockPtr++), 0x38);

    // Check encoding for: NULL FLAG(0x00) + "2035-07-04"=0x80005D75(23925 days in big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[4]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x5D);
    ASSERT_EQ(*(keyBlockPtr++), 0x75);

    checkTupleIdxAndFactorizedTableIdx(1, keyBlockPtr);

    // Check encoding for: NULL FLAG(0x00) + -412414=0x7FFFFFFFFFF9B502(big endian).
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 4; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0xF9);
    ASSERT_EQ(*(keyBlockPtr++), 0xB5);
    ASSERT_EQ(*(keyBlockPtr++), 0x02);

    checkNullVal(keyBlockPtr, DOUBLE, isAscOrder[1]);

    // Check encoding for: "short str".
    checkNonNullFlag(keyBlockPtr, isAscOrder[2]);
    ASSERT_EQ(*(keyBlockPtr++), 's');
    ASSERT_EQ(*(keyBlockPtr++), 'h');
    ASSERT_EQ(*(keyBlockPtr++), 'o');
    ASSERT_EQ(*(keyBlockPtr++), 'r');
    ASSERT_EQ(*(keyBlockPtr++), 't');
    ASSERT_EQ(*(keyBlockPtr++), ' ');
    ASSERT_EQ(*(keyBlockPtr++), 's');
    ASSERT_EQ(*(keyBlockPtr++), 't');
    ASSERT_EQ(*(keyBlockPtr++), 'r');
    ASSERT_EQ(*(keyBlockPtr++), '\0');
    ASSERT_EQ(*(keyBlockPtr++), '\0');
    ASSERT_EQ(*(keyBlockPtr++), '\0');
    checkLongStrFlag(keyBlockPtr, isAscOrder[2], false /* isLongStr */);

    checkNullVal(keyBlockPtr, TIMESTAMP, isAscOrder[3]);

    checkNullVal(keyBlockPtr, DATE, isAscOrder[4]);

    checkTupleIdxAndFactorizedTableIdx(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, multipleOrderByColMultiBlockTest) {
    const auto numOfOrderByCols = 10;
    const auto numOfElementsPerCol = 2000;
    auto valueVectors = getInt64TestValueVector(numOfElementsPerCol, numOfOrderByCols, true);
    auto isAscOrder = vector<bool>(numOfOrderByCols, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, memoryManager.get(), ftIdx,
        numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(valueVectors));
    for (auto i = 0u; i < numOfElementsPerCol; i++) {
        orderByKeyEncoder.encodeKeys();
        valueVectors[0]->state->currIdx++;
    }
    checkKeyBlockForInt64TestValueVector(valueVectors, orderByKeyEncoder.getKeyBlocks(),
        numOfElementsPerCol, isAscOrder, orderByKeyEncoder.getMaxNumTuplesPerBlock());
}
