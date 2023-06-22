#include <numeric>
#include <variant>
#include <vector>

#include "common/assert.h"
#include "common/constants.h"
#include "common/data_chunk/data_chunk.h"
#include "gtest/gtest.h"
#include "processor/operator/order_by/order_by_key_encoder.h"
#include "processor/operator/order_by/radix_sort.h"

using ::testing::Test;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;

class RadixSortTest : public Test {

public:
    void SetUp() override {
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::STORAGE);
        bufferManager = std::make_unique<BufferManager>(
            BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = std::make_unique<MemoryManager>(bufferManager.get());
    }

    void TearDown() override {
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::STORAGE);
    }

public:
    std::unique_ptr<BufferManager> bufferManager;
    std::unique_ptr<MemoryManager> memoryManager;
    const uint8_t factorizedTableIdx = 9;
    const uint32_t numTuplesPerBlockInFT = BufferPoolConstants::PAGE_256KB_SIZE / 8;

    void checkTupleIdxesAndFactorizedTableIdxes(uint8_t* keyBlockPtr, const uint64_t entrySize,
        const std::vector<uint64_t>& expectedFTBlockOffsetOrder) {
        for (auto expectedFTBlockOffset : expectedFTBlockOffsetOrder) {
            auto tupleInfoPtr = keyBlockPtr + entrySize - 8;
            ASSERT_EQ(OrderByKeyEncoder::getEncodedFTIdx(tupleInfoPtr), factorizedTableIdx);
            ASSERT_EQ(OrderByKeyEncoder::getEncodedFTBlockIdx(tupleInfoPtr), 0);
            auto encodedFTBlockOffset = OrderByKeyEncoder::getEncodedFTBlockOffset(tupleInfoPtr);
            if (expectedFTBlockOffset != -1) {
                ASSERT_EQ(encodedFTBlockOffset, expectedFTBlockOffset);
            } else {
                // For tuples with the same value, we just need to check the tuple id is valid and
                // in the range of [0, expectedFTBlockOffsetOrder.size()).
                ASSERT_EQ((0 <= encodedFTBlockOffset) &&
                              (encodedFTBlockOffset < expectedFTBlockOffsetOrder.size()),
                    true);
            }
            keyBlockPtr += entrySize;
        }
    }

    void sortAllKeyBlocks(OrderByKeyEncoder& orderByKeyEncoder, RadixSort& radixSort) {
        for (auto& keyBlock : orderByKeyEncoder.getKeyBlocks()) {
            radixSort.sortSingleKeyBlock(*keyBlock);
        }
    }

    template<typename T>
    void singleOrderByColTest(const std::vector<T>& sortingData, const std::vector<bool>& nullMasks,
        const std::vector<uint64_t>& expectedBlockOffsetOrder, const LogicalTypeID dataTypeID,
        const bool isAsc, bool hasPayLoadCol) {
        KU_ASSERT(sortingData.size() == nullMasks.size());
        KU_ASSERT(sortingData.size() == expectedBlockOffsetOrder.size());
        auto dataChunk = std::make_shared<DataChunk>(hasPayLoadCol ? 2 : 1);
        dataChunk->state->selVector->selectedSize = sortingData.size();
        auto valueVector = std::make_shared<ValueVector>(dataTypeID, memoryManager.get());
        for (auto i = 0u; i < dataChunk->state->selVector->selectedSize; i++) {
            if (nullMasks[i]) {
                valueVector->setNull(i, true);
            } else {
                valueVector->setValue<T>(i, sortingData[i]);
            }
        }
        dataChunk->insert(0, valueVector);
        std::vector<ValueVector*> orderByVectors{
            valueVector.get()}; // only contains order_by columns
        std::vector<ValueVector*> allVectors{
            valueVector.get()}; // all columns including order_by and payload columns
        std::vector<bool> isAscOrder{isAsc};

        std::unique_ptr<FactorizedTableSchema> tableSchema =
            std::make_unique<FactorizedTableSchema>();
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(false /* isUnflat */,
            0 /* dataChunkPos */, LogicalTypeUtils::getRowLayoutSize(LogicalType{dataTypeID})));
        std::vector<StrKeyColInfo> strKeyColsInfo;

        if (hasPayLoadCol) {
            // Create a new payloadValueVector for the payload column.
            auto payloadValueVector =
                std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager.get());
            for (auto i = 0u; i < dataChunk->state->selVector->selectedSize; i++) {
                payloadValueVector->setValue(i, std::to_string(i));
            }
            dataChunk->insert(1, payloadValueVector);
            // To test whether the orderByCol -> ftIdx works properly, we put the
            // payload column at index 0, and the orderByCol at index 1.
            allVectors.insert(allVectors.begin(), payloadValueVector.get());
            tableSchema->appendColumn(std::make_unique<ColumnSchema>(false /* isUnflat */,
                0 /* dataChunkPos */, LogicalTypeUtils::getRowLayoutSize(LogicalType{dataTypeID})));
            strKeyColsInfo.emplace_back(
                StrKeyColInfo(tableSchema->getColOffset(1) /* colOffsetInFT */,
                    0 /* colOffsetInEncodedKeyBlock */, isAsc));
        } else if constexpr (std::is_same<T, std::string>::value) {
            // If this is a string column and has no payload column, then the
            // factorizedTable offset is just 0.
            strKeyColsInfo.emplace_back(
                StrKeyColInfo(tableSchema->getColOffset(0) /* colOffsetInFT */,
                    0 /* colOffsetInEncodedKeyBlock */, isAsc));
        }

        FactorizedTable factorizedTable(memoryManager.get(), std::move(tableSchema));
        factorizedTable.append(allVectors);

        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, memoryManager.get(), factorizedTableIdx,
                numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(orderByVectors));
        orderByKeyEncoder.encodeKeys();

        RadixSort radixSort =
            RadixSort(memoryManager.get(), factorizedTable, orderByKeyEncoder, strKeyColsInfo);
        sortAllKeyBlocks(orderByKeyEncoder, radixSort);

        checkTupleIdxesAndFactorizedTableIdxes(orderByKeyEncoder.getKeyBlocks()[0]->getData(),
            orderByKeyEncoder.getNumBytesPerTuple(), expectedBlockOffsetOrder);
    }

    void multipleOrderByColSolveTieTest(std::vector<bool>& isAscOrder,
        std::vector<uint64_t>& expectedBlockOffsetOrder,
        std::vector<std::vector<std::string>>& stringValues) {
        std::vector<ValueVector*> orderByVectors;
        auto mockDataChunk = std::make_shared<DataChunk>(stringValues.size());
        mockDataChunk->state->currIdx = 0;
        std::unique_ptr<FactorizedTableSchema> tableSchema =
            std::make_unique<FactorizedTableSchema>();
        std::vector<StrKeyColInfo> strKeyColsInfo;
        for (auto i = 0; i < stringValues.size(); i++) {
            auto stringValueVector =
                std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager.get());
            tableSchema->appendColumn(std::make_unique<ColumnSchema>(
                false /* isUnflat */, 0 /* dataChunkPos */, sizeof(ku_string_t)));
            strKeyColsInfo.push_back(StrKeyColInfo(tableSchema->getColOffset(strKeyColsInfo.size()),
                strKeyColsInfo.size() *
                    OrderByKeyEncoder::getEncodingSize(stringValueVector->dataType),
                isAscOrder[i]));
            mockDataChunk->insert(i, stringValueVector);
            for (auto j = 0u; j < stringValues[i].size(); j++) {
                stringValueVector->setValue(j, stringValues[i][j]);
            }
            orderByVectors.emplace_back(stringValueVector.get());
        }

        FactorizedTable factorizedTable(memoryManager.get(), std::move(tableSchema));

        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, memoryManager.get(), factorizedTableIdx,
                numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(orderByVectors));
        mockDataChunk->state->selVector->resetSelectorToValuePosBuffer();
        mockDataChunk->state->selVector->selectedSize = 1;
        for (auto i = 0u; i < expectedBlockOffsetOrder.size(); i++) {
            mockDataChunk->state->selVector->selectedPositions[0] = i;
            factorizedTable.append(orderByVectors);
            orderByKeyEncoder.encodeKeys();
            mockDataChunk->state->currIdx++;
        }
        mockDataChunk->state->selVector->resetSelectorToUnselected();

        auto radixSort =
            RadixSort(memoryManager.get(), factorizedTable, orderByKeyEncoder, strKeyColsInfo);
        sortAllKeyBlocks(orderByKeyEncoder, radixSort);

        checkTupleIdxesAndFactorizedTableIdxes(orderByKeyEncoder.getKeyBlocks()[0]->getData(),
            orderByKeyEncoder.getNumBytesPerTuple(), expectedBlockOffsetOrder);
    }
};

TEST_F(RadixSortTest, singleOrderByColInt64Test) {
    std::vector<int64_t> sortingData = {73 /* positive 1 byte number */, 0 /* NULL */,
        -132 /* negative 1 byte number */, -5242 /* negative 2 bytes number */, INT64_MAX,
        INT64_MIN, 210042 /* positive 2 bytes number */};
    std::vector<bool> nullMasks = {false, true, false, false, false, false, false};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {5, 3, 2, 0, 6, 4, 1};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::INT64,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColNoNullInt64Test) {
    std::vector<int64_t> sortingData = {48 /* positive 1 byte number */,
        39842 /* positive 2 bytes number */, -1 /* negative 1 byte number */,
        -819321 /* negative 2 bytes number */, INT64_MAX, INT64_MIN};
    std::vector<bool> nullMasks(6, false);
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {4, 1, 0, 2, 3, 5};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::INT64,
        false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColLargeInputInt64Test) {
    // 240 is the maximum number of tuples we can put into a memory block
    // since: 4096 / (9 + 8) = 240.
    std::vector<int64_t> sortingData(240);
    iota(sortingData.begin(), sortingData.end(), 0);
    reverse(sortingData.begin(), sortingData.end());
    std::vector<bool> nullMasks(240, false);
    std::vector<uint64_t> expectedFTBlockOffsetOrder(240);
    iota(expectedFTBlockOffsetOrder.begin(), expectedFTBlockOffsetOrder.end(), 0);
    reverse(expectedFTBlockOffsetOrder.begin(), expectedFTBlockOffsetOrder.end());
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::INT64,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColBoolTest) {
    std::vector<int64_t> sortingData = {true, false, false /* NULL */};
    std::vector<bool> nullMasks = {false, false, true};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {2, 0, 1};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::BOOL,
        false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColDateTest) {
    std::vector<date_t> sortingData = {
        Date::FromCString("1970-01-01", strlen("1970-01-01")) /* days=0 */,
        Date::FromCString("1970-01-02", strlen("1970-01-02")) /* positive days */,
        Date::FromCString("2003-10-12", strlen("2003-10-12")) /* large positive days */,
        Date::FromCString("1968-12-21", strlen("1968-12-21")) /* negative days */,
        date_t(0) /*NULL*/};
    std::vector<bool> nullMasks = {false, false, false, false, true};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {3, 0, 1, 2, 4};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::DATE,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColTimestampTest) {
    std::vector<timestamp_t> sortingData = {
        Timestamp::FromCString("1970-01-01 00:00:00", strlen("1970-01-01 00:00:00")) /* micros=0 */,
        Timestamp::FromCString(
            "1970-01-02 14:21:11", strlen("1970-01-02 14:21:11")) /* positive micros */,
        timestamp_t(0) /*NULL*/,
        Timestamp::FromCString(
            "2003-10-12 08:21:10", strlen("2003-10-12 08:21:10")) /* large positive micros */,
        Timestamp::FromCString(
            "1959-03-20 11:12:13.500", strlen("1959-03-20 11:12:13.500")) /* negative micros */
    };

    std::vector<bool> nullMasks = {false, false, true, false, false};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {2, 3, 1, 0, 4};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder,
        LogicalTypeID::TIMESTAMP, false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColIntervalTest) {
    // We need to normalize days and micros in intervals.
    std::vector<interval_t> sortingData = {
        interval_t(0, 0, 0) /* NULL */,
        Interval::FromCString(
            "100 days 3 years 2 hours 178 minutes", strlen("100 days 3 years 2 hours 178 minutes")),
        Interval::FromCString("2 years 466 days 20 minutes",
            strlen("2 years 466 days 20 minutes")) /* =3 years 106 days 20 minutes */,
        Interval::FromCString("3 years 99 days 200 hours 100 minutes",
            strlen("3 years 99 days 100 hours 100 minutes")) /* =3 years 107 days 8 hours 100
                                                                minutes */
        ,
    };

    std::vector<bool> nullMasks = {true, false, false, false};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {0, 3, 2, 1};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder,
        LogicalTypeID::INTERVAL, false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColDoubleTest) {
    std::vector<double> sortingData = {0.0123 /* small positive number */,
        -0.90123 /* small negative number */, 95152 /* large positive number */,
        -76123 /* large negative number */, 0, 0 /* NULL */};
    std::vector<bool> nullMasks = {false, false, false, false, false, true};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {5, 2, 0, 4, 1, 3};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::DOUBLE,
        false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColStringTest) {
    // Multiple groups of string with the same prefix generates multiple groups of ties during radix
    // sort.
    std::vector<std::string> sortingData = {"abcdef", "other common prefix test1",
        "another common prefix2", "common prefix rank1", "common prefix rank3",
        "common prefix rank2", "another common prefix1", "another short string", "" /*NULL*/};
    std::vector<bool> nullMasks = {false, false, false, false, false, false, false, false, true};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {0, 6, 2, 7, 3, 5, 4, 1, 8};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::STRING,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColNoNullStringTest) {
    // Multiple groups of string with the same prefix generates multiple groups of ties during radix
    // sort.
    std::vector<std::string> sortingData = {"simple short", "other common prefix test2",
        "another common prefix2", "common prefix rank1", "common prefix rank3",
        "common prefix rank2", "other common prefix test3", "another short string"};
    std::vector<bool> nullMasks(8, false);
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {0, 6, 1, 4, 5, 3, 7, 2};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::STRING,
        false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColAllTiesStringTest) {
    // All the strings are the same, so there is a tie across all tuples and the tie can't be
    // solved. The tuple ordering depends on the c++ std::sort, so we just need to check that the
    // tupleIdx is valid and is in the range of [0~19).
    std::vector<std::string> sortingData(20, "same string for all tuples");
    std::vector<bool> nullMasks(20, false);
    std::vector<uint64_t> expectedFTBlockOffsetOrder(20, -1);
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::STRING,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, singleOrderByColWithPayloadTest) {
    // The first column is a payload column and the second column is an order_by column. The radix
    // sort needs to use the factorizedTableColIdx to correctly read the strings.
    std::vector<std::string> sortingData = {"string column with payload col test5",
        "string column with payload col test3", "string 1",
        "string column with payload col long long", "very long long long string"};
    std::vector<bool> nullMasks(5, false);
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {2, 3, 1, 0, 4};
    singleOrderByColTest(sortingData, nullMasks, expectedFTBlockOffsetOrder, LogicalTypeID::STRING,
        true /* isAsc */, true /* hasPayLoadCol */);
}

TEST_F(RadixSortTest, multipleOrderByColNoTieTest) {
    std::vector<bool> isAscOrder = {true, false, true, false, false};
    auto intFlatValueVector =
        std::make_shared<ValueVector>(LogicalTypeID::INT64, memoryManager.get());
    auto doubleFlatValueVector =
        std::make_shared<ValueVector>(LogicalTypeID::DOUBLE, memoryManager.get());
    auto stringFlatValueVector =
        std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager.get());
    auto timestampFlatValueVector =
        std::make_shared<ValueVector>(LogicalTypeID::TIMESTAMP, memoryManager.get());
    auto dateFlatValueVector =
        std::make_shared<ValueVector>(LogicalTypeID::DATE, memoryManager.get());

    auto mockDataChunk = std::make_shared<DataChunk>(5);
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

    std::vector<ValueVector*> orderByVectors{intFlatValueVector.get(), doubleFlatValueVector.get(),
        stringFlatValueVector.get(), timestampFlatValueVector.get(), dateFlatValueVector.get()};
    intFlatValueVector->setValue(0, (int64_t)41);
    intFlatValueVector->setValue(1, (int64_t)-132);
    intFlatValueVector->setValue(2, (int64_t)41);
    intFlatValueVector->setNull(3, true);
    intFlatValueVector->setValue(4, (int64_t)0);
    doubleFlatValueVector->setValue(0, (double_t)453.421);
    doubleFlatValueVector->setValue(1, (double_t)-415.23);
    doubleFlatValueVector->setValue(2, (double_t)-0.00421);
    doubleFlatValueVector->setValue(3, (double_t)0);
    doubleFlatValueVector->setValue(4, (double_t)0.0121);
    stringFlatValueVector->setValue<std::string>(0, "common prefix2");
    stringFlatValueVector->setValue<std::string>(1, "common prefix1");
    stringFlatValueVector->setValue<std::string>(2, "common prefix");
    stringFlatValueVector->setNull(3, true);
    stringFlatValueVector->setValue<std::string>(4, "short str");
    timestampFlatValueVector->setValue(
        0, Timestamp::FromCString("1970-01-01 00:00:00", strlen("1970-01-01 00:00:00")));
    timestampFlatValueVector->setValue(
        1, Timestamp::FromCString("1962-04-07 14:11:23", strlen("1962-04-07 14:11:23")));
    timestampFlatValueVector->setValue(
        2, Timestamp::FromCString("1970-01-01 01:00:00", strlen("1970-01-01 01:00:00")));
    timestampFlatValueVector->setValue(
        3, Timestamp::FromCString("1953-01-12 21:12:00", strlen("2053-01-12 21:12:00")));
    timestampFlatValueVector->setNull(4, true);
    dateFlatValueVector->setValue(0, Date::FromCString("1978-09-12", strlen("1978-09-12")));
    dateFlatValueVector->setValue(1, Date::FromCString("2035-07-04", strlen("2035-07-04")));
    dateFlatValueVector->setNull(2, true);
    dateFlatValueVector->setValue(3, Date::FromCString("1964-01-21", strlen("1964-01-21")));
    dateFlatValueVector->setValue(4, Date::FromCString("2000-11-13", strlen("2000-11-13")));

    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::INT64})));
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::DOUBLE})));
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::TIMESTAMP})));
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::DATE})));

    FactorizedTable factorizedTable(memoryManager.get(), std::move(tableSchema));
    std::vector<StrKeyColInfo> strKeyColsInfo = {StrKeyColInfo(16 /* colOffsetInFT */,
        OrderByKeyEncoder::getEncodingSize(LogicalType(LogicalTypeID::INT64)) +
            OrderByKeyEncoder::getEncodingSize(LogicalType(LogicalTypeID::DOUBLE)),
        true /* isAscOrder */)};

    auto orderByKeyEncoder =
        OrderByKeyEncoder(orderByVectors, isAscOrder, memoryManager.get(), factorizedTableIdx,
            numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(orderByVectors));
    mockDataChunk->state->selVector->resetSelectorToValuePosBuffer();
    mockDataChunk->state->selVector->selectedSize = 1;
    for (auto i = 0u; i < 5; i++) {
        mockDataChunk->state->selVector->selectedPositions[0] = i;
        orderByKeyEncoder.encodeKeys();
        factorizedTable.append(orderByVectors);
        mockDataChunk->state->currIdx++;
    }
    mockDataChunk->state->selVector->resetSelectorToUnselected();

    RadixSort radixSort =
        RadixSort(memoryManager.get(), factorizedTable, orderByKeyEncoder, strKeyColsInfo);
    sortAllKeyBlocks(orderByKeyEncoder, radixSort);

    std::vector<uint64_t> expectedFTBlockOffsetOrder = {1, 4, 0, 2, 3};
    checkTupleIdxesAndFactorizedTableIdxes(orderByKeyEncoder.getKeyBlocks()[0]->getData(),
        orderByKeyEncoder.getNumBytesPerTuple(), expectedFTBlockOffsetOrder);
}

TEST_F(RadixSortTest, multipleOrderByColSolvableTieTest) {
    std::vector<bool> isAscOrder = {false, true};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {
        4,
        0,
        1,
        3,
        2,
    };
    // The first column has ties, need to compare the second column to solve the tie. However there
    // are still some ties that are not solvable.
    std::vector<std::vector<std::string>> stringValues = {
        {"same common prefix different1", "same common prefix different",
            "same common prefix different", "same common prefix different",
            "same common prefix different1"},
        {"second same common prefix2", "second same common prefix0", "second same common prefix3",
            "second same common prefix2", "second same common prefix1"}};
    multipleOrderByColSolveTieTest(isAscOrder, expectedFTBlockOffsetOrder, stringValues);
}

TEST_F(RadixSortTest, multipleOrderByColUnSolvableTieTest) {
    std::vector<bool> isAscOrder = {true, true};
    std::vector<uint64_t> expectedFTBlockOffsetOrder = {1, 3, 2, 0, 4};
    // The first column has ties, need to compare the second column to solve the tie. However there
    // are still some ties that are not solvable.
    std::vector<std::vector<std::string>> stringValues = {
        {"same common prefix different1", "same common prefix different",
            "same common prefix different", "same common prefix different",
            "same common prefix different1"},
        {"second same common prefix2", "second same common prefix0", "second same common prefix3",
            "second same common prefix0", "second same common prefix2"}};
    multipleOrderByColSolveTieTest(isAscOrder, expectedFTBlockOffsetOrder, stringValues);
}
