#include <vector>

#include "gtest/gtest.h"

#include "src/common/include/configs.h"
#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/date.h"
#include "src/common/include/interval.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

using ::testing::Test;
using namespace graphflow::processor;
using namespace std;

class OrderByKeyEncoderTest : public Test {

public:
    void SetUp() override { memoryManager = make_unique<MemoryManager>(); }

    void checkRowID(uint64_t rowID, uint8_t*& keyBlockPtr) {
        ASSERT_EQ(*(uint64_t*)keyBlockPtr, rowID);
        keyBlockPtr += 8;
    }

    // this method can only be used to check the null flag for a not null value.
    // we should call checkNullVal directly to check a null value
    inline void checkNonNullFlag(uint8_t*& keyBlockPtr, bool isAsc) {
        ASSERT_EQ(*(keyBlockPtr++), isAsc ? 0x00 : 0xFF);
    }

    // if the col is in asc order, the encoding string is:
    // 0xFF(null flag) + 0xFF...FF(padding)
    // if the col is in desc order, the encoding string is:
    // 0x00(null flag) + 0x00...00(padding)
    inline void checkNullVal(uint8_t*& keyBlockPtr, DataType dataType, bool isAsc) {
        for (auto i = 0u; i < OrderByKeyEncoder::getEncodingSize(dataType); i++) {
            ASSERT_EQ(*(keyBlockPtr++), isAsc ? 0xFF : 0x00);
        }
    }

    // this function generates a ValueVector of int64 entries that are all 5
    vector<shared_ptr<ValueVector>> getInt64TestValueVector(
        const uint64_t numOfElementsPerCol, const uint64_t numOfOrderByCols, bool flatCol) {
        shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(numOfOrderByCols);
        dataChunk->state->selectedSize = numOfElementsPerCol;
        vector<shared_ptr<ValueVector>> valueVectors;
        for (auto i = 0u; i < numOfOrderByCols; i++) {
            shared_ptr<ValueVector> valueVector =
                make_shared<ValueVector>(memoryManager.get(), INT64);
            auto intValuePtr = (int64_t*)valueVector->values;
            for (auto j = 0u; j < numOfElementsPerCol; j++) {
                intValuePtr[j] = 5;
            }
            dataChunk->insert(i, valueVector);
            valueVector->state->currIdx = flatCol ? 0 : -1;
            valueVectors.emplace_back(valueVector);
        }
        return valueVectors;
    }

    // this function assumes that all columns have datatype: INT64, and each entry is 5
    void checkKeyBlockForInt64TestValueVector(vector<shared_ptr<ValueVector>>& valueVectors,
        vector<unique_ptr<MemoryBlock>>& keyBlocks, uint64_t numOfElements,
        vector<bool>& isAscOrder) {
        const auto entrySize = OrderByKeyEncoder::getEntrySize(valueVectors);
        const auto numEntriesPerBlock = SORT_BLOCK_SIZE / entrySize;
        for (auto i = 0u; i < keyBlocks.size(); i++) {
            auto numOfElementsToCheck = min(numOfElements, numEntriesPerBlock);
            numOfElements -= numOfElementsToCheck;
            auto keyBlockPtr = keyBlocks[i]->data;
            for (auto j = 0u; j < numOfElementsToCheck; j++) {
                for (auto k = 0u; k < valueVectors.size(); k++) {
                    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
                    if (isAscOrder[0]) {
                        // check encoding for: NULL FLAG(0x00) + 5=0x8000000000000005(big endian)
                        ASSERT_EQ(*(keyBlockPtr++), 0x80);
                        for (auto k = 0u; k < 6; k++) {
                            ASSERT_EQ(*(keyBlockPtr++), 0x00);
                        }
                        ASSERT_EQ(*(keyBlockPtr++), 0x05);
                    } else {
                        // check encoding for: NULL FLAG(0xFF) + 5=0x7FFFFFFFFFFFFFFFFA(big endian)
                        // note: we need to flip all bits since this column is in descending order
                        ASSERT_EQ(*(keyBlockPtr++), 0x7F);
                        for (auto k = 0u; k < 6; k++) {
                            ASSERT_EQ(*(keyBlockPtr++), 0xFF);
                        }
                        ASSERT_EQ(*(keyBlockPtr++), 0xFA);
                    }
                }
                checkRowID(i * numEntriesPerBlock + j, keyBlockPtr);
            }
        }
    }

    void singleOrderByColMultiBlockTest(bool isFlat) {
        uint64_t numOfElements = 2000;
        auto valueVectors = getInt64TestValueVector(numOfElements, 1, isFlat);
        auto isAscOrder = vector<bool>(1, false);
        auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
        if (isFlat) {
            for (auto i = 0u; i < numOfElements; i++) {
                orderByKeyEncoder.encodeKeys();
                valueVectors[0]->state->currIdx++;
            }
        } else {
            orderByKeyEncoder.encodeKeys();
        }
        checkKeyBlockForInt64TestValueVector(
            valueVectors, orderByKeyEncoder.getKeyBlocks(), numOfElements, isAscOrder);
    }

public:
    shared_ptr<MemoryManager> memoryManager;
};

TEST_F(OrderByKeyEncoderTest, singleOrderByColInt64UnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 6;
    shared_ptr<ValueVector> int64ValueVector = make_shared<ValueVector>(memoryManager.get(), INT64);
    auto int64Values = (int64_t*)int64ValueVector->values;
    int64Values[0] = 73; // positive number
    int64ValueVector->setNull(1, true);
    int64Values[2] = -132;  // negative 1 byte number
    int64Values[3] = -5242; // negative 2 bytes number
    int64Values[4] = INT64_MAX;
    int64Values[5] = INT64_MIN;
    dataChunk->insert(0, int64ValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(int64ValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // check encoding for: NULL FLAG(0x00) + 73=0x8000000000000049(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0x00);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x49);
    checkRowID(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, INT64, isAscOrder[0]);
    checkRowID(1, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + -132=0x7FFFFFFFFFFFFF7C(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x7C);
    checkRowID(2, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + -5242=0x7FFFFFFFFFFFEB86(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 5; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0xEB);
    ASSERT_EQ(*(keyBlockPtr++), 0x86);
    checkRowID(3, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + INT64_MAX=0xFFFFFFFFFFFFFFFF(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    for (auto i = 0U; i < 8; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    checkRowID(4, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + INT64_MIN=0x0000000000000000(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    for (auto i = 0u; i < 8; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0x00);
    }
    checkRowID(5, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColBoolUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 3;
    shared_ptr<ValueVector> boolValueVector = make_shared<ValueVector>(memoryManager.get(), BOOL);
    auto boolValues = (bool*)boolValueVector->values;
    boolValues[0] = true;
    boolValues[1] = false;
    boolValueVector->setNull(2, true);
    dataChunk->insert(0, boolValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(boolValueVector);
    auto isAscOrder = vector<bool>(1, false);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // check encoding for: NULL FLAG(0x00) + true=0xFE(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xFE);
    checkRowID(0, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + false=0xFF(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    checkRowID(1, keyBlockPtr);

    checkNullVal(keyBlockPtr, BOOL, isAscOrder[0]);
    checkRowID(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColDateUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 3;
    shared_ptr<ValueVector> dateValueVector = make_shared<ValueVector>(memoryManager.get(), DATE);
    auto dateValues = (date_t*)dateValueVector->values;
    dateValues[0] = Date::FromCString("2035-07-04", strlen("2035-07-04")); // date after 1970-01-01
    dateValueVector->setNull(1, true);
    dateValues[2] = Date::FromCString("1949-10-01", strlen("1949-10-01")); // date before 1970-01-01
    dataChunk->insert(0, dateValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(dateValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // check encoding for: NULL FLAG(0x00) + "2035-07-04"=0x80005D75(23925 days in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x5D);
    ASSERT_EQ(*(keyBlockPtr++), 0x75);
    checkRowID(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, DATE, isAscOrder[0]);
    checkRowID(1, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + "1949-10-01"=0x7FFFE31B(-7397 days in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    ASSERT_EQ(*(keyBlockPtr++), 0xE3);
    ASSERT_EQ(*(keyBlockPtr++), 0x1B);
    checkRowID(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColTimestampUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 3;
    shared_ptr<ValueVector> timestampValueVector =
        make_shared<ValueVector>(memoryManager.get(), TIMESTAMP);
    auto timestampValues = (timestamp_t*)timestampValueVector->values;
    // timestamp before 1970-01-01
    timestampValues[0] =
        Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123"));
    timestampValueVector->setNull(1, true);
    // timestamp after 1970-01-01
    timestampValues[2] =
        Timestamp::FromCString("2035-07-01 11:14:33", strlen("2035-07-01 11:14:33"));
    dataChunk->insert(0, timestampValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(timestampValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // check encoding for: NULL FLAG(0x00) + "1962-04-07 11:12:35.123"=0x7FFF21F7F9D08F38
    // (-244126044877000 micros in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    ASSERT_EQ(*(keyBlockPtr++), 0x21);
    ASSERT_EQ(*(keyBlockPtr++), 0xF7);
    ASSERT_EQ(*(keyBlockPtr++), 0xF9);
    ASSERT_EQ(*(keyBlockPtr++), 0xD0);
    ASSERT_EQ(*(keyBlockPtr++), 0x8F);
    ASSERT_EQ(*(keyBlockPtr++), 0x38);
    checkRowID(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, TIMESTAMP, isAscOrder[0]);
    checkRowID(1, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + "2035-07-01 11:14:33"=0x800757D5F429B840
    // (2066901273000000 micros in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x07);
    ASSERT_EQ(*(keyBlockPtr++), 0x57);
    ASSERT_EQ(*(keyBlockPtr++), 0xD5);
    ASSERT_EQ(*(keyBlockPtr++), 0xF4);
    ASSERT_EQ(*(keyBlockPtr++), 0x29);
    ASSERT_EQ(*(keyBlockPtr++), 0xB8);
    ASSERT_EQ(*(keyBlockPtr++), 0x40);
    checkRowID(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColIntervalUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 2;
    shared_ptr<ValueVector> intervalValueVector =
        make_shared<ValueVector>(memoryManager.get(), INTERVAL);
    auto intervalValues = (interval_t*)intervalValueVector->values;
    intervalValues[0] = Interval::FromCString("18 hours 55 days 13 years 8 milliseconds 3 months",
        strlen("18 hours 55 days 13 years 8 milliseconds 3 months"));
    intervalValueVector->setNull(1, true);
    dataChunk->insert(0, intervalValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(intervalValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // check encoding for: NULL FLAG(0x00) +  "18 hours 55 days 13 years 8 milliseconds 3 months"
    // = NULL FLAG(0x00) + 159 months(0x8000009F) + 55 days(0x80000037)
    // + 64800008000 micros(0x8000000F1661A740)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    // check for months: 159 (0x8000009F in big endian)
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x9F);
    // check for days: 55 (0x80000037 in big endian)
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x37);
    // check for micros: 64800008000 (0x8000000F1661A740 in big endian)
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x0F);
    ASSERT_EQ(*(keyBlockPtr++), 0x16);
    ASSERT_EQ(*(keyBlockPtr++), 0x61);
    ASSERT_EQ(*(keyBlockPtr++), 0xA7);
    ASSERT_EQ(*(keyBlockPtr++), 0x40);
    checkRowID(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, INTERVAL, isAscOrder[0]);
    checkRowID(1, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColStringUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 4;
    shared_ptr<ValueVector> stringValueVector =
        make_shared<ValueVector>(memoryManager.get(), STRING);
    stringValueVector->addString(0, "short str"); // short string
    stringValueVector->setNull(1, true);
    stringValueVector->addString(2, "commonprefix string1"); // long string(encoding: commonprefix)
    stringValueVector->addString(3, "commonprefix string2"); // long string(encoding: commonprefix)
    dataChunk->insert(0, stringValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(stringValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // check encoding for: NULL FLAG(0x00) + "short str"
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
    checkRowID(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, STRING, isAscOrder[0]);
    checkRowID(1, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + "commonprefix string1"
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
    checkRowID(2, keyBlockPtr);

    // check encoding for val: NULL FLAG(0x00) + "commonprefix string2"
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
    checkRowID(3, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColDoubleUnflatTest) {
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = 6;
    shared_ptr<ValueVector> doubleValueVector =
        make_shared<ValueVector>(memoryManager.get(), DOUBLE);
    auto doubleValues = (double*)doubleValueVector->values;
    doubleValues[0] = 3.452; // small positive number
    doubleValueVector->setNull(1, true);
    doubleValues[2] = -0.00031213;     // very small negative number
    doubleValues[3] = -5.42113;        // small negative number
    doubleValues[4] = 92931312341415;  // large positive number
    doubleValues[5] = -31234142783434; // large negative number
    dataChunk->insert(0, doubleValueVector);
    vector<shared_ptr<ValueVector>> valueVectors;
    valueVectors.emplace_back(doubleValueVector);
    auto isAscOrder = vector<bool>(1, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    orderByKeyEncoder.encodeKeys();
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // check encoding for: NULL FLAG(0x00) + 3.452=0xC00B9DB22D0E5604(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xC0);
    ASSERT_EQ(*(keyBlockPtr++), 0x0B);
    ASSERT_EQ(*(keyBlockPtr++), 0x9D);
    ASSERT_EQ(*(keyBlockPtr++), 0xB2);
    ASSERT_EQ(*(keyBlockPtr++), 0x2D);
    ASSERT_EQ(*(keyBlockPtr++), 0x0E);
    ASSERT_EQ(*(keyBlockPtr++), 0x56);
    ASSERT_EQ(*(keyBlockPtr++), 0x04);
    checkRowID(0, keyBlockPtr);

    checkNullVal(keyBlockPtr, INT64, isAscOrder[0]);
    checkRowID(1, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + -0.00031213=0x40CB8B53DB9F4D8D(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x40);
    ASSERT_EQ(*(keyBlockPtr++), 0xCB);
    ASSERT_EQ(*(keyBlockPtr++), 0x8B);
    ASSERT_EQ(*(keyBlockPtr++), 0x53);
    ASSERT_EQ(*(keyBlockPtr++), 0xDB);
    ASSERT_EQ(*(keyBlockPtr++), 0x9F);
    ASSERT_EQ(*(keyBlockPtr++), 0x4D);
    ASSERT_EQ(*(keyBlockPtr++), 0x8D);
    checkRowID(2, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + -5.42113=0x3FEA50C34C1A8AC5(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x3F);
    ASSERT_EQ(*(keyBlockPtr++), 0xEA);
    ASSERT_EQ(*(keyBlockPtr++), 0x50);
    ASSERT_EQ(*(keyBlockPtr++), 0xC3);
    ASSERT_EQ(*(keyBlockPtr++), 0x4C);
    ASSERT_EQ(*(keyBlockPtr++), 0x1A);
    ASSERT_EQ(*(keyBlockPtr++), 0x8A);
    ASSERT_EQ(*(keyBlockPtr++), 0xC5);
    checkRowID(3, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + 92931312341415=0xC2D52150771469C0(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0xC2);
    ASSERT_EQ(*(keyBlockPtr++), 0xD5);
    ASSERT_EQ(*(keyBlockPtr++), 0x21);
    ASSERT_EQ(*(keyBlockPtr++), 0x50);
    ASSERT_EQ(*(keyBlockPtr++), 0x77);
    ASSERT_EQ(*(keyBlockPtr++), 0x14);
    ASSERT_EQ(*(keyBlockPtr++), 0x69);
    ASSERT_EQ(*(keyBlockPtr++), 0xC0);
    checkRowID(4, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + -31234142783434=0x3D4397BC03B835FF(big endian)
    // check encoding for val:
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x3D);
    ASSERT_EQ(*(keyBlockPtr++), 0x43);
    ASSERT_EQ(*(keyBlockPtr++), 0x97);
    ASSERT_EQ(*(keyBlockPtr++), 0xBC);
    ASSERT_EQ(*(keyBlockPtr++), 0x03);
    ASSERT_EQ(*(keyBlockPtr++), 0xB8);
    ASSERT_EQ(*(keyBlockPtr++), 0x35);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    checkRowID(5, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, largeEntrySizeErrorTest) {
    // if the entry size is larger than 4096 bytes, the encoder will raise an encoding exception
    // we need 241(note: (4096 - 8) / 9  + 1 = 455) columns(with datatype INTERVAL) to trigger that
    // exception
    uint32_t numOfOrderByCols = 455;
    auto valueVectors = getInt64TestValueVector(1, numOfOrderByCols, true);
    auto isAscOrder = vector<bool>(numOfOrderByCols, true);
    try {
        auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
        FAIL();
    } catch (EncodingException& e) {
        ASSERT_STREQ(e.what(), "OrderBy encoder exception: EntrySize(4103 bytes) is larger than "
                               "the SORT_BLOCK_SIZE(4096 bytes)");
    } catch (exception& e) { FAIL(); }
}

TEST_F(OrderByKeyEncoderTest, singleEntryPerBlockTest) {
    // if the entry size is between 4KB~2KB, each block can only contain one entry
    // entry size is: 9(int64 encoding size) * 300 + 8(rowID) = 2708(bytes) for 300 int64 columns
    uint32_t numOfOrderByCols = 300;
    uint32_t numOfElementsPerCol = 10;
    auto valueVectors = getInt64TestValueVector(numOfElementsPerCol, numOfOrderByCols, true);
    auto isAscOrder = vector<bool>(numOfOrderByCols, false);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    for (auto i = 0u; i < numOfElementsPerCol; i++) {
        orderByKeyEncoder.encodeKeys();
        valueVectors[0]->state->currIdx++;
    }
    auto& keyBlocks = orderByKeyEncoder.getKeyBlocks();
    checkKeyBlockForInt64TestValueVector(valueVectors, keyBlocks, numOfElementsPerCol, isAscOrder);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColMultiBlockUnflatTest) {
    singleOrderByColMultiBlockTest(false);
}

TEST_F(OrderByKeyEncoderTest, singleOrderByColMultiBlockFlatTest) {
    singleOrderByColMultiBlockTest(true);
}

TEST_F(OrderByKeyEncoderTest, multipleOrderByColSingleBlockTest) {
    vector<bool> isAscOrder = {true, false, true, true, true};
    auto intFlatValueVector = make_shared<ValueVector>(memoryManager.get(), INT64);
    auto doubleFlatValueVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
    auto stringFlatValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    auto timestampFlatValueVector = make_shared<ValueVector>(memoryManager.get(), TIMESTAMP);
    auto dateFlatValueVector = make_shared<ValueVector>(memoryManager.get(), DATE);

    auto mockDataChunk = make_shared<DataChunk>(5);
    mockDataChunk->insert(0, intFlatValueVector);
    mockDataChunk->insert(1, doubleFlatValueVector);
    mockDataChunk->insert(2, stringFlatValueVector);
    mockDataChunk->insert(3, timestampFlatValueVector);
    mockDataChunk->insert(4, dateFlatValueVector);

    auto intValues = (int64_t*)intFlatValueVector->values;
    auto doubleValues = (double*)doubleFlatValueVector->values;
    auto timestampValues = (timestamp_t*)timestampFlatValueVector->values;
    auto dateValues = (date_t*)dateFlatValueVector->values;

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

    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    uint8_t* keyBlockPtr = orderByKeyEncoder.getKeyBlocks()[0]->data;

    // first row
    intValues[0] = 73;
    intValues[1] = -132;
    intValues[2] = -412414;
    doubleValues[0] = 53.421;
    doubleValues[1] = -415.23;
    doubleFlatValueVector->setNull(2, true);
    stringFlatValueVector->setNull(0, true);
    stringFlatValueVector->addString(1, "this is a test string!!");
    stringFlatValueVector->addString(2, "short str");
    timestampValues[0] =
        Timestamp::FromCString("2008-08-08 20:20:20", strlen("2008-08-08 20:20:20"));
    timestampValues[1] =
        Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123"));
    timestampFlatValueVector->setNull(2, true);
    dateValues[0] = Date::FromCString("1978-09-12", strlen("1978-09-12"));
    dateValues[1] = Date::FromCString("2035-07-04", strlen("2035-07-04"));
    dateFlatValueVector->setNull(2, true);
    for (auto i = 0u; i < 3; i++) {
        orderByKeyEncoder.encodeKeys();
        mockDataChunk->state->currIdx++;
    }

    // check encoding for: NULL FLAG(0x00) + 73=0x8000000000000049(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0x00);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x49);

    // check encoding for: NULL FLAG(0x00) + 53.421=0x3FB54A1CAC083126(big endian)
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

    // check encoding for: NULL FLAG(0x00) + "2008-08-08 20:20:20"=0x800453F888DCA900
    // (1218226820000000 micros in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[3]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x04);
    ASSERT_EQ(*(keyBlockPtr++), 0x53);
    ASSERT_EQ(*(keyBlockPtr++), 0xF8);
    ASSERT_EQ(*(keyBlockPtr++), 0x88);
    ASSERT_EQ(*(keyBlockPtr++), 0xDC);
    ASSERT_EQ(*(keyBlockPtr++), 0xA9);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);

    // check encoding for: NULL FLAG(0x00) + "1978-09-12"=0x80000C68(3176 days in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[4]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x0C);
    ASSERT_EQ(*(keyBlockPtr++), 0x68);

    checkRowID(0, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + -132=0x7FFFFFFFFFFFFF7C(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 6; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0x7C);

    // check encoding for: NULL FLAG(0x00) + -415.23=0xC079F3AE147AE148(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[1]);
    ASSERT_EQ(*(keyBlockPtr++), 0xC0);
    ASSERT_EQ(*(keyBlockPtr++), 0x79);
    ASSERT_EQ(*(keyBlockPtr++), 0xF3);
    ASSERT_EQ(*(keyBlockPtr++), 0xAE);
    ASSERT_EQ(*(keyBlockPtr++), 0x14);
    ASSERT_EQ(*(keyBlockPtr++), 0x7A);
    ASSERT_EQ(*(keyBlockPtr++), 0xE1);
    ASSERT_EQ(*(keyBlockPtr++), 0x48);

    // check encoding for: "this is a test string!!"
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

    // check encoding for: NULL FLAG(0x00) + "1962-04-07 11:12:35.123"=0x7FFF21F7F9D08F38
    // (-244126044877000 micros in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[3]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    ASSERT_EQ(*(keyBlockPtr++), 0x21);
    ASSERT_EQ(*(keyBlockPtr++), 0xF7);
    ASSERT_EQ(*(keyBlockPtr++), 0xF9);
    ASSERT_EQ(*(keyBlockPtr++), 0xD0);
    ASSERT_EQ(*(keyBlockPtr++), 0x8F);
    ASSERT_EQ(*(keyBlockPtr++), 0x38);

    // check encoding for: NULL FLAG(0x00) + "2035-07-04"=0x80005D75(23925 days in big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[4]);
    ASSERT_EQ(*(keyBlockPtr++), 0x80);
    ASSERT_EQ(*(keyBlockPtr++), 0x00);
    ASSERT_EQ(*(keyBlockPtr++), 0x5D);
    ASSERT_EQ(*(keyBlockPtr++), 0x75);

    checkRowID(1, keyBlockPtr);

    // check encoding for: NULL FLAG(0x00) + -412414=0x7FFFFFFFFFF9B502(big endian)
    checkNonNullFlag(keyBlockPtr, isAscOrder[0]);
    ASSERT_EQ(*(keyBlockPtr++), 0x7F);
    for (auto i = 0u; i < 4; i++) {
        ASSERT_EQ(*(keyBlockPtr++), 0xFF);
    }
    ASSERT_EQ(*(keyBlockPtr++), 0xF9);
    ASSERT_EQ(*(keyBlockPtr++), 0xB5);
    ASSERT_EQ(*(keyBlockPtr++), 0x02);

    checkNullVal(keyBlockPtr, DOUBLE, isAscOrder[1]);

    // check encoding for: "short str"
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

    checkNullVal(keyBlockPtr, TIMESTAMP, isAscOrder[3]);

    checkNullVal(keyBlockPtr, DATE, isAscOrder[4]);

    checkRowID(2, keyBlockPtr);
}

TEST_F(OrderByKeyEncoderTest, multipleOrderByColMultiBlockTest) {
    const auto numOfOrderByCols = 10;
    const auto numOfElementsPerCol = 2000;
    auto valueVectors = getInt64TestValueVector(numOfElementsPerCol, numOfOrderByCols, true);
    auto isAscOrder = vector<bool>(numOfOrderByCols, true);
    auto orderByKeyEncoder = OrderByKeyEncoder(valueVectors, isAscOrder, *memoryManager);
    for (auto i = 0u; i < numOfElementsPerCol; i++) {
        orderByKeyEncoder.encodeKeys();
        valueVectors[0]->state->currIdx++;
    }
    checkKeyBlockForInt64TestValueVector(
        valueVectors, orderByKeyEncoder.getKeyBlocks(), numOfElementsPerCol, isAscOrder);
}
