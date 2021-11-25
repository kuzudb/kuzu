#include <numeric>
#include <variant>
#include <vector>

#include "gtest/gtest.h"

#include "src/common/include/assert.h"
#include "src/common/include/configs.h"
#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/date.h"
#include "src/common/include/interval.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"
#include "src/processor/include/physical_plan/operator/order_by/radix_sort.h"

using ::testing::Test;
using namespace graphflow::processor;
using namespace std;

class RadixSortTest : public Test {

public:
    void SetUp() override { memoryManager = make_unique<MemoryManager>(); }

public:
    unique_ptr<MemoryManager> memoryManager;

    void checkRowIDs(uint8_t* keyBlockPtr, const uint64_t entrySize,
        const vector<uint64_t>& expectedRowIDOrder) {
        for (auto expectedRowID : expectedRowIDOrder) {
            auto rowID = *((uint64_t*)(keyBlockPtr + entrySize - sizeof(uint64_t)));
            if (expectedRowID != -1) {
                ASSERT_EQ(rowID, expectedRowID);
            } else {
                // for rows with the same value, we just need to check the row id is valid and
                // in the range of [0, expectedRowIDOrder.size())
                ASSERT_EQ((0 <= rowID) && (rowID < expectedRowIDOrder.size()), true);
            }
            keyBlockPtr += entrySize;
        }
    }

    template<typename T>
    void singleOrderByColTest(const vector<T>& sortingData, const vector<bool>& nullMasks,
        const vector<uint64_t>& expectedRowIDOrder, const DataType dataType, const bool isAsc) {
        GF_ASSERT(sortingData.size() == nullMasks.size());
        GF_ASSERT(sortingData.size() == expectedRowIDOrder.size());
        auto dataChunk = make_shared<DataChunk>(1);
        dataChunk->state->selectedSize = sortingData.size();
        auto valueVector = make_shared<ValueVector>(memoryManager.get(), dataType);
        auto values = (T*)valueVector->values;
        for (auto i = 0u; i < dataChunk->state->selectedSize; i++) {
            if (nullMasks[i]) {
                valueVector->setNull(i, true);
            } else if constexpr (is_same<T, string>::value) {
                valueVector->addString(i, sortingData[i]);
            } else {
                values[i] = sortingData[i];
            }
        }
        dataChunk->insert(0, valueVector);
        vector<shared_ptr<ValueVector>> orderByVectors{valueVector};
        vector<bool> isAscOrder{isAsc};
        auto orderByKeyEncoder = OrderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager);
        orderByKeyEncoder.encodeKeys();
        auto& keyBlocks = orderByKeyEncoder.getKeyBlocks();
        auto& keyBlockToSort = keyBlocks[0];
        RadixSort radixSort = RadixSort(orderByVectors, *memoryManager, isAscOrder);
        radixSort.sortKeyBlock(*keyBlockToSort, dataChunk->state->selectedSize);
        checkRowIDs(keyBlockToSort->data, OrderByKeyEncoder::getEntrySize(orderByVectors),
            expectedRowIDOrder);
    }

    void multipleOrderByColSolveTieTest(vector<bool>& isAscOrder,
        vector<uint64_t>& expectedRowIDOrder, vector<vector<string>>& stringValues) {
        vector<shared_ptr<ValueVector>> orderByVectors;
        auto mockDataChunk = make_shared<DataChunk>(stringValues.size());
        mockDataChunk->state->currIdx = 0;
        for (auto i = 0; i < stringValues.size(); i++) {
            auto stringValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
            mockDataChunk->insert(i, stringValueVector);
            for (auto j = 0u; j < stringValues[i].size(); j++) {
                stringValueVector->addString(j, stringValues[i][j]);
            }
            orderByVectors.emplace_back(stringValueVector);
        }

        auto orderByKeyEncoder = OrderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager);
        for (auto i = 0u; i < expectedRowIDOrder.size(); i++) {
            orderByKeyEncoder.encodeKeys();
            mockDataChunk->state->currIdx++;
        }
        auto& keyBlocks = orderByKeyEncoder.getKeyBlocks();
        auto& keyBlockToSort = keyBlocks[0];
        RadixSort radixSort = RadixSort(orderByVectors, *memoryManager, isAscOrder);
        radixSort.sortKeyBlock(*keyBlockToSort, expectedRowIDOrder.size());
        checkRowIDs(keyBlocks[0]->data, OrderByKeyEncoder::getEntrySize(orderByVectors),
            expectedRowIDOrder);
    }
};

TEST_F(RadixSortTest, singleOrderByColInt64Test) {
    vector<int64_t> sortingData = {73 /*positive 1 byte number*/, 0 /*NULL*/,
        -132 /*negative 1 byte number*/, -5242 /*negative 2 bytes number*/, INT64_MAX, INT64_MIN,
        210042 /*positive 2 bytes number*/};
    vector<bool> nullMasks = {false, true, false, false, false, false, false};
    vector<uint64_t> expectedRowIDOrder = {5, 3, 2, 0, 6, 4, 1};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, INT64, true);
}

TEST_F(RadixSortTest, singleOrderByColNoNullInt64Test) {
    vector<int64_t> sortingData = {48 /*positive 1 byte number*/, 39842 /*positive 2 bytes number*/,
        -1 /*negative 1 byte number*/, -819321 /*negative 2 bytes number*/, INT64_MAX, INT64_MIN};

    vector<bool> nullMasks(6, false);
    vector<uint64_t> expectedRowIDOrder = {4, 1, 0, 2, 3, 5};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, INT64, false);
}

TEST_F(RadixSortTest, singleOrderByColLargeInputInt64Test) {
    // 240 is the maximum number of rows we can put into a memory block
    // since: 4096 / (9 + 8) = 240
    vector<int64_t> sortingData(240);
    iota(sortingData.begin(), sortingData.end(), 0);
    reverse(sortingData.begin(), sortingData.end());
    vector<bool> nullMasks(240, false);
    vector<uint64_t> expectedRowIDOrder(240);
    iota(expectedRowIDOrder.begin(), expectedRowIDOrder.end(), 0);
    reverse(expectedRowIDOrder.begin(), expectedRowIDOrder.end());
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, INT64, true);
}

TEST_F(RadixSortTest, singleOrderByColBoolTest) {
    vector<int64_t> sortingData = {true, false, false /*NULL*/};
    vector<bool> nullMasks = {false, false, true};
    vector<uint64_t> expectedRowIDOrder = {2, 0, 1};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, BOOL, false);
}

TEST_F(RadixSortTest, singleOrderByColDateTest) {
    vector<date_t> sortingData = {Date::FromCString("1970-01-01", strlen("1970-01-01")) /*days=0*/,
        Date::FromCString("1970-01-02", strlen("1970-01-02")) /*positive days*/,
        Date::FromCString("2003-10-12", strlen("2003-10-12")) /*large positive days*/,
        Date::FromCString("1968-12-21", strlen("1968-12-21")) /*negative days*/,
        date_t(0) /*NULL*/};
    vector<bool> nullMasks = {false, false, false, false, true};
    vector<uint64_t> expectedRowIDOrder = {3, 0, 1, 2, 4};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, DATE, true);
}

TEST_F(RadixSortTest, singleOrderByColTimestampTest) {
    vector<timestamp_t> sortingData = {
        Timestamp::FromCString("1970-01-01 00:00:00", strlen("1970-01-01 00:00:00")) /*micros=0*/,
        Timestamp::FromCString(
            "1970-01-02 14:21:11", strlen("1970-01-02 14:21:11")) /*positive micros*/,
        timestamp_t(0) /*NULL*/,
        Timestamp::FromCString(
            "2003-10-12 08:21:10", strlen("2003-10-12 08:21:10")) /*large positive micros*/,
        Timestamp::FromCString(
            "1959-03-20 11:12:13.500", strlen("1959-03-20 11:12:13.500")) /*negative micros*/
    };

    vector<bool> nullMasks = {false, false, true, false, false};
    vector<uint64_t> expectedRowIDOrder = {2, 3, 1, 0, 4};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, TIMESTAMP, false);
}

TEST_F(RadixSortTest, singleOrderByColIntervalTest) {
    // we need to normalize days and micros in intervals.
    vector<interval_t> sortingData = {
        interval_t(0, 0, 0) /*NULL*/,
        Interval::FromCString(
            "100 days 3 years 2 hours 178 minutes", strlen("100 days 3 years 2 hours 178 minutes")),
        Interval::FromCString("2 years 466 days 20 minutes",
            strlen("2 years 466 days 20 minutes")) /*=3 years 106 days 20 minutes*/,
        Interval::FromCString("3 years 99 days 200 hours 100 minutes",
            strlen(
                "3 years 99 days 100 hours 100 minutes")) /*=3 years 107 days 8 hours 100 minutes*/,
    };

    vector<bool> nullMasks = {true, false, false, false};
    vector<uint64_t> expectedRowIDOrder = {0, 3, 2, 1};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, INTERVAL, false);
}

TEST_F(RadixSortTest, singleOrderByColDoubleTest) {
    vector<double> sortingData = {0.0123 /*small positive number*/,
        -0.90123 /*small negative number */, 95152 /*large positive number*/,
        -76123 /*large negative number*/, 0, 0 /*NULL*/};
    vector<bool> nullMasks = {false, false, false, false, false, true};
    vector<uint64_t> expectedRowIDOrder = {5, 2, 0, 4, 1, 3};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, DOUBLE, false);
}

TEST_F(RadixSortTest, singleOrderByColStringTest) {
    // multiple groups of string with the same prefix generates multiple groups of ties during radix
    // sort
    vector<string> sortingData = {"abcdef", "other common prefix test1", "another common prefix2",
        "common prefix rank1", "common prefix rank3", "common prefix rank2",
        "another common prefix1", "another short string", "" /*NULL*/};
    vector<bool> nullMasks = {false, false, false, false, false, false, false, false, true};
    vector<uint64_t> expectedRowIDOrder = {0, 6, 2, 7, 3, 5, 4, 1, 8};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, STRING, true);
}

TEST_F(RadixSortTest, singleOrderByColNoNullStringTest) {
    // multiple groups of string with the same prefix generates multiple groups of ties during radix
    // sort
    vector<string> sortingData = {"simple short", "other common prefix test2",
        "another common prefix2", "common prefix rank1", "common prefix rank3",
        "common prefix rank2", "other common prefix test3", "another short string"};
    vector<bool> nullMasks(8, false);
    vector<uint64_t> expectedRowIDOrder = {0, 6, 1, 4, 5, 3, 7, 2};
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, STRING, false);
}

TEST_F(RadixSortTest, singleOrderByColAllTiesStringTest) {
    // all the strings are the same, so there is a tie across all rows and the tie can't be solved
    // the row ordering depends on the c++ std::sort, so we just need to check that the row id is
    // valid and is in the range of [0~19)
    vector<string> sortingData(20, "same string for all rows");
    vector<bool> nullMasks(20, false);
    vector<uint64_t> expectedRowIDOrder(20, -1);
    singleOrderByColTest(sortingData, nullMasks, expectedRowIDOrder, STRING, true);
}

TEST_F(RadixSortTest, multipleOrderByColNoTieTest) {
    vector<bool> isAscOrder = {true, false, true, false, false};
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

    vector<shared_ptr<ValueVector>> orderByVectors{intFlatValueVector, doubleFlatValueVector,
        stringFlatValueVector, timestampFlatValueVector, dateFlatValueVector};
    intValues[0] = 41;
    intValues[1] = -132;
    intValues[2] = 41;
    intFlatValueVector->setNull(3, true);
    intValues[4] = 0;
    doubleValues[0] = 453.421;
    doubleValues[1] = -415.23;
    doubleValues[2] = -0.00421;
    doubleValues[3] = 0;
    doubleValues[4] = 0.0121;
    stringFlatValueVector->addString(0, "common prefix2");
    stringFlatValueVector->addString(1, "common prefix1");
    stringFlatValueVector->addString(2, "common prefix");
    stringFlatValueVector->setNull(3, true);
    stringFlatValueVector->addString(4, "short str");
    timestampValues[0] =
        Timestamp::FromCString("1970-01-01 00:00:00", strlen("1970-01-01 00:00:00"));
    timestampValues[1] =
        Timestamp::FromCString("1962-04-07 14:11:23", strlen("1962-04-07 14:11:23"));
    timestampValues[2] =
        Timestamp::FromCString("1970-01-01 01:00:00", strlen("1970-01-01 01:00:00"));
    timestampValues[3] =
        Timestamp::FromCString("1953-01-12 21:12:00", strlen("2053-01-12 21:12:00"));
    timestampFlatValueVector->setNull(4, true);
    dateValues[0] = Date::FromCString("1978-09-12", strlen("1978-09-12"));
    dateValues[1] = Date::FromCString("2035-07-04", strlen("2035-07-04"));
    dateFlatValueVector->setNull(2, true);
    dateValues[3] = Date::FromCString("1964-01-21", strlen("1964-01-21"));
    dateValues[4] = Date::FromCString("2000-11-13", strlen("2000-11-13"));

    auto orderByKeyEncoder = OrderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager);
    for (auto i = 0u; i < 5; i++) {
        orderByKeyEncoder.encodeKeys();
        mockDataChunk->state->currIdx++;
    }

    auto& keyBlocks = orderByKeyEncoder.getKeyBlocks();
    auto& keyBlockToSort = keyBlocks[0];
    RadixSort radixSort = RadixSort(orderByVectors, *memoryManager, isAscOrder);
    radixSort.sortKeyBlock(*keyBlockToSort, 5);
    vector<uint64_t> expectedRowIDOrder = {1, 4, 0, 2, 3};
    checkRowIDs(
        keyBlocks[0]->data, OrderByKeyEncoder::getEntrySize(orderByVectors), expectedRowIDOrder);
}

TEST_F(RadixSortTest, multipleOrderByColSolvableTieTest) {
    vector<bool> isAscOrder = {false, true};
    vector<uint64_t> expectedRowIDOrder = {
        4,
        0,
        1,
        3,
        2,
    };
    // The first column has ties, need to compare the second column to solve the tie. However there
    // are still some ties that are not solvable
    vector<vector<string>> stringValues = {
        {"same common prefix different1", "same common prefix different",
            "same common prefix different", "same common prefix different",
            "same common prefix different1"},
        {"second same common prefix2", "second same common prefix0", "second same common prefix3",
            "second same common prefix2", "second same common prefix1"}};
    multipleOrderByColSolveTieTest(isAscOrder, expectedRowIDOrder, stringValues);
}

TEST_F(RadixSortTest, multipleOrderByColUnSolvableTieTest) {
    vector<bool> isAscOrder = {true, true};
    vector<uint64_t> expectedRowIDOrder = {1, 3, 2, 0, 4};
    // The first column has ties, need to compare the second column to solve the tie. However there
    // are still some ties that are not solvable
    vector<vector<string>> stringValues = {
        {"same common prefix different1", "same common prefix different",
            "same common prefix different", "same common prefix different",
            "same common prefix different1"},
        {"second same common prefix2", "second same common prefix0", "second same common prefix3",
            "second same common prefix0", "second same common prefix2"}};
    multipleOrderByColSolveTieTest(isAscOrder, expectedRowIDOrder, stringValues);
}
