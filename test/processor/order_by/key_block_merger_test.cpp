#include <numeric>
#include <variant>
#include <vector>

#include "common/assert.h"
#include "common/constants.h"
#include "common/data_chunk/data_chunk.h"
#include "gtest/gtest.h"
#include "processor/operator/order_by/key_block_merger.h"
#include "processor/operator/order_by/order_by_key_encoder.h"

using ::testing::Test;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;

class KeyBlockMergerTest : public Test {

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
    uint32_t numTuplesPerBlockInFT = BufferPoolConstants::PAGE_256KB_SIZE / 8;

    static void checkTupleIdxesAndFactorizedTableIdxes(uint8_t* keyBlockPtr,
        const uint64_t keyBlockEntrySizeInBytes,
        const std::vector<uint64_t>& expectedBlockOffsetOrder,
        const std::vector<uint64_t>& expectedFactorizedTableIdxOrder) {
        assert(expectedBlockOffsetOrder.size() == expectedFactorizedTableIdxOrder.size());
        for (auto i = 0u; i < expectedBlockOffsetOrder.size(); i++) {
            auto tupleInfoPtr = keyBlockPtr + keyBlockEntrySizeInBytes - sizeof(uint64_t);
            ASSERT_EQ(OrderByKeyEncoder::getEncodedFTBlockIdx(tupleInfoPtr), 0);
            ASSERT_EQ(OrderByKeyEncoder::getEncodedFTBlockOffset(tupleInfoPtr),
                expectedBlockOffsetOrder[i]);
            ASSERT_EQ(OrderByKeyEncoder::getEncodedFTIdx(tupleInfoPtr),
                expectedFactorizedTableIdxOrder[i]);
            keyBlockPtr += keyBlockEntrySizeInBytes;
        }
    }

    template<typename T>
    OrderByKeyEncoder prepareSingleOrderByColEncoder(const std::vector<T>& sortingData,
        const std::vector<bool>& nullMasks, LogicalTypeID dataTypeID, bool isAsc,
        uint16_t factorizedTableIdx, bool hasPayLoadCol,
        std::vector<std::shared_ptr<FactorizedTable>>& factorizedTables,
        std::shared_ptr<DataChunk>& dataChunk) {
        KU_ASSERT(sortingData.size() == nullMasks.size());
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

        std::unique_ptr<FactorizedTableSchema> tableSchema =
            std::make_unique<FactorizedTableSchema>();
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(false /* isUnflat */,
            0 /* dataChunkPos */, LogicalTypeUtils::getRowLayoutSize(LogicalType{dataTypeID})));

        if (hasPayLoadCol) {
            auto payloadValueVector =
                std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager.get());
            for (auto i = 0u; i < dataChunk->state->selVector->selectedSize; i++) {
                payloadValueVector->setValue(i, std::to_string(i));
            }
            dataChunk->insert(1, payloadValueVector);
            // To test whether the orderByCol -> factorizedTableColIdx works properly, we put the
            // payload column at index 0, and the orderByCol at index 1.
            allVectors.insert(allVectors.begin(), payloadValueVector.get());
            tableSchema->appendColumn(std::make_unique<ColumnSchema>(false, 0 /* dataChunkPos */,
                LogicalTypeUtils::getRowLayoutSize(LogicalType{dataTypeID})));
        }

        auto factorizedTable =
            std::make_unique<FactorizedTable>(memoryManager.get(), std::move(tableSchema));
        factorizedTable->append(allVectors);

        std::vector<bool> isAscOrder = {isAsc};
        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, memoryManager.get(), factorizedTableIdx,
                numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(orderByVectors));
        orderByKeyEncoder.encodeKeys();

        factorizedTables.emplace_back(std::move(factorizedTable));
        return orderByKeyEncoder;
    }

    template<typename T>
    void singleOrderByColMergeTest(const std::vector<T>& leftSortingData,
        const std::vector<bool>& leftNullMasks, const std::vector<T>& rightSortingData,
        const std::vector<bool>& rightNullMasks,
        const std::vector<uint64_t>& expectedBlockOffsetOrder,
        const std::vector<uint64_t>& expectedFactorizedTableIdxOrder,
        const LogicalTypeID dataTypeID, const bool isAsc, bool hasPayLoadCol) {
        std::vector<std::shared_ptr<FactorizedTable>> factorizedTables;
        auto dataChunk0 = std::make_shared<DataChunk>(hasPayLoadCol ? 2 : 1);
        auto dataChunk1 = std::make_shared<DataChunk>(hasPayLoadCol ? 2 : 1);
        auto orderByKeyEncoder1 = prepareSingleOrderByColEncoder(leftSortingData, leftNullMasks,
            dataTypeID, isAsc, 0 /* ftIdx */, hasPayLoadCol, factorizedTables, dataChunk0);
        auto orderByKeyEncoder2 = prepareSingleOrderByColEncoder(rightSortingData, rightNullMasks,
            dataTypeID, isAsc, 1 /* ftIdx */, hasPayLoadCol, factorizedTables, dataChunk1);

        std::vector<StrKeyColInfo> strKeyColsInfo;
        if (hasPayLoadCol) {
            strKeyColsInfo.emplace_back(
                StrKeyColInfo(8 /* colOffsetInFT */, 0 /* colOffsetInEncodedKeyBlock */, isAsc));
        } else if constexpr (std::is_same<T, std::string>::value) {
            strKeyColsInfo.emplace_back(
                StrKeyColInfo(0 /* colOffsetInFT */, 0 /* colOffsetInEncodedKeyBlock */, isAsc));
        }

        KeyBlockMerger keyBlockMerger = KeyBlockMerger(
            factorizedTables, strKeyColsInfo, orderByKeyEncoder1.getNumBytesPerTuple());

        auto numBytesPerEntry = orderByKeyEncoder1.getNumBytesPerTuple();
        auto resultKeyBlock = std::make_shared<MergedKeyBlocks>(numBytesPerEntry,
            leftSortingData.size() + rightSortingData.size(), memoryManager.get());
        auto keyBlockMergeTask =
            std::make_shared<KeyBlockMergeTask>(std::make_shared<MergedKeyBlocks>(numBytesPerEntry,
                                                    orderByKeyEncoder1.getKeyBlocks()[0]),
                std::make_shared<MergedKeyBlocks>(
                    numBytesPerEntry, orderByKeyEncoder2.getKeyBlocks()[0]),
                resultKeyBlock, keyBlockMerger);
        KeyBlockMergeMorsel keyBlockMergeMorsel(
            0, leftSortingData.size(), 0, rightSortingData.size());
        keyBlockMergeMorsel.keyBlockMergeTask = keyBlockMergeTask;

        keyBlockMerger.mergeKeyBlocks(keyBlockMergeMorsel);

        checkTupleIdxesAndFactorizedTableIdxes(resultKeyBlock->getTuple(0),
            orderByKeyEncoder1.getNumBytesPerTuple(), expectedBlockOffsetOrder,
            expectedFactorizedTableIdxOrder);
    }

    OrderByKeyEncoder prepareMultipleOrderByColsEncoder(uint16_t factorizedTableIdx,
        std::vector<std::shared_ptr<FactorizedTable>>& factorizedTables,
        std::shared_ptr<DataChunk>& dataChunk, std::unique_ptr<FactorizedTableSchema> tableSchema) {
        std::vector<ValueVector*> orderByVectors;
        for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
            orderByVectors.emplace_back(dataChunk->getValueVector(i).get());
        }

        std::vector<bool> isAscOrder(orderByVectors.size(), true);
        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, memoryManager.get(), factorizedTableIdx,
                numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(orderByVectors));

        auto factorizedTable =
            std::make_unique<FactorizedTable>(memoryManager.get(), std::move(tableSchema));
        // Manually flatten the data chunk.
        dataChunk->state->selVector->resetSelectorToValuePosBuffer();
        dataChunk->state->selVector->selectedSize = 1;
        for (auto i = 0u; i < dataChunk->state->originalSize; i++) {
            dataChunk->state->selVector->selectedPositions[0] = i;
            factorizedTable->append(orderByVectors);
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }
        dataChunk->state->selVector->resetSelectorToUnselected();

        factorizedTables.emplace_back(std::move(factorizedTable));
        return orderByKeyEncoder;
    }

    void prepareMultipleOrderByColsValueVector(std::vector<int64_t>& int64Values,
        std::vector<double>& doubleValues, std::vector<timestamp_t>& timestampValues,
        std::shared_ptr<DataChunk>& dataChunk) {
        assert(int64Values.size() == doubleValues.size());
        assert(doubleValues.size() == timestampValues.size());
        dataChunk->state->initOriginalAndSelectedSize(int64Values.size());
        dataChunk->state->currIdx = 0;

        auto int64ValueVector =
            std::make_shared<ValueVector>(LogicalTypeID::INT64, memoryManager.get());
        auto doubleValueVector =
            std::make_shared<ValueVector>(LogicalTypeID::DOUBLE, memoryManager.get());
        auto timestampValueVector =
            std::make_shared<ValueVector>(LogicalTypeID::TIMESTAMP, memoryManager.get());

        dataChunk->insert(0, int64ValueVector);
        dataChunk->insert(1, doubleValueVector);
        dataChunk->insert(2, timestampValueVector);

        for (auto i = 0u; i < int64Values.size(); i++) {
            int64ValueVector->setValue(i, int64Values[i]);
            doubleValueVector->setValue(i, doubleValues[i]);
            timestampValueVector->setValue(i, timestampValues[i]);
        }
    }

    void multipleOrderByColTest(bool hasStrCol) {
        std::vector<int64_t> int64Values1 = {INT64_MIN, -78, 23};
        std::vector<double> doubleValues1 = {3.28, -0.0001, 4.621};
        std::vector<timestamp_t> timestampValues1 = {
            Timestamp::FromCString("2035-07-01 11:14:33", strlen("2035-07-01 11:14:33")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123"))};
        auto dataChunk1 = std::make_shared<DataChunk>(3 + (hasStrCol ? 1 : 0));
        prepareMultipleOrderByColsValueVector(
            int64Values1, doubleValues1, timestampValues1, dataChunk1);

        std::vector<int64_t> int64Values2 = {INT64_MIN, -78, 23, INT64_MAX};
        std::vector<double> doubleValues2 = {0.58, -0.0001, 4.621, 4.621};
        std::vector<timestamp_t> timestampValues2 = {
            Timestamp::FromCString("2036-07-01 11:14:33", strlen("2036-07-01 11:14:33")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")),
            Timestamp::FromCString("2035-07-01 11:14:33", strlen("2035-07-01 11:14:33"))};
        auto dataChunk2 = std::make_shared<DataChunk>(3 + (hasStrCol ? 1 : 0));
        prepareMultipleOrderByColsValueVector(
            int64Values2, doubleValues2, timestampValues2, dataChunk2);

        std::unique_ptr<FactorizedTableSchema> tableSchema =
            std::make_unique<FactorizedTableSchema>();
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
                LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::INT64})));
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
                LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::DOUBLE})));
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
                LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::TIMESTAMP})));

        if (hasStrCol) {
            tableSchema->appendColumn(
                std::make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */,
                    LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
            auto stringValueVector1 =
                std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager.get());
            auto stringValueVector2 =
                std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager.get());
            dataChunk1->insert(3, stringValueVector1);
            dataChunk2->insert(3, stringValueVector2);

            stringValueVector1->setValue<std::string>(0, "same prefix 123");
            stringValueVector1->setValue<std::string>(1, "same prefix 128");
            stringValueVector1->setValue<std::string>(2, "same prefix 123");

            stringValueVector2->setValue<std::string>(0, "same prefix 127");
            stringValueVector2->setValue<std::string>(1, "same prefix 123");
            stringValueVector2->setValue<std::string>(2, "same prefix 121");
            stringValueVector2->setValue<std::string>(3, "same prefix 126");
        }

        std::vector<std::shared_ptr<FactorizedTable>> factorizedTables;
        for (auto i = 0; i < 4; i++) {
            factorizedTables.emplace_back(std::make_unique<FactorizedTable>(
                memoryManager.get(), std::make_unique<FactorizedTableSchema>(*tableSchema)));
        }
        auto orderByKeyEncoder2 = prepareMultipleOrderByColsEncoder(4 /* ftIdx */, factorizedTables,
            dataChunk2, std::make_unique<FactorizedTableSchema>(*tableSchema));
        auto orderByKeyEncoder1 = prepareMultipleOrderByColsEncoder(5 /* ftIdx */, factorizedTables,
            dataChunk1, std::make_unique<FactorizedTableSchema>(*tableSchema));

        std::vector<uint64_t> expectedBlockOffsetOrder = {0, 0, 1, 1, 2, 2, 3};
        std::vector<uint64_t> expectedFactorizedTableIdxOrder = {4, 5, 5, 4, 5, 4, 4};

        std::vector<StrKeyColInfo> strKeyColsInfo;
        if (hasStrCol) {
            strKeyColsInfo.emplace_back(StrKeyColInfo(
                tableSchema->getColOffset(3 /* colIdx */) /* colOffsetInFT */,
                LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::INT64}) +
                    LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::DOUBLE}) +
                    LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::TIMESTAMP}) + 3,
                true /* isAscOrder */));
            expectedBlockOffsetOrder = {0, 0, 1, 1, 2, 2, 3};
            expectedFactorizedTableIdxOrder = {4, 5, 4, 5, 4, 5, 4};
        }

        auto numBytesPerEntry = orderByKeyEncoder1.getNumBytesPerTuple();
        KeyBlockMerger keyBlockMerger = KeyBlockMerger(
            factorizedTables, strKeyColsInfo, orderByKeyEncoder1.getNumBytesPerTuple());
        auto resultKeyBlock =
            std::make_shared<MergedKeyBlocks>(numBytesPerEntry, 7ul, memoryManager.get());
        auto keyBlockMergeTask =
            std::make_shared<KeyBlockMergeTask>(std::make_shared<MergedKeyBlocks>(numBytesPerEntry,
                                                    orderByKeyEncoder1.getKeyBlocks()[0]),
                std::make_shared<MergedKeyBlocks>(
                    numBytesPerEntry, orderByKeyEncoder2.getKeyBlocks()[0]),
                resultKeyBlock, keyBlockMerger);
        KeyBlockMergeMorsel keyBlockMergeMorsel(0, 3, 0, 4);
        keyBlockMergeMorsel.keyBlockMergeTask = keyBlockMergeTask;

        keyBlockMerger.mergeKeyBlocks(keyBlockMergeMorsel);

        checkTupleIdxesAndFactorizedTableIdxes(resultKeyBlock->getTuple(0),
            orderByKeyEncoder1.getNumBytesPerTuple(), expectedBlockOffsetOrder,
            expectedFactorizedTableIdxOrder);
    }

    OrderByKeyEncoder prepareMultipleStrKeyColsEncoder(std::shared_ptr<DataChunk>& dataChunk,
        std::vector<std::vector<std::string>>& strValues, uint16_t factorizedTableIdx,
        std::vector<std::shared_ptr<FactorizedTable>>& factorizedTables) {
        dataChunk->state->currIdx = 0;
        dataChunk->state->initOriginalAndSelectedSize(strValues[0].size());
        for (auto i = 0u; i < strValues.size(); i++) {
            auto strValueVector =
                std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager.get());
            dataChunk->insert(i, strValueVector);
            for (auto j = 0u; j < strValues[i].size(); j++) {
                strValueVector->setValue(j, strValues[i][j]);
            }
        }

        // The first, second, fourth columns are keyColumns.
        std::vector<ValueVector*> orderByVectors{dataChunk->getValueVector(0).get(),
            dataChunk->getValueVector(1).get(), dataChunk->getValueVector(3).get()};

        std::vector<ValueVector*> allVectors{dataChunk->getValueVector(0).get(),
            dataChunk->getValueVector(1).get(), dataChunk->getValueVector(2).get(),
            dataChunk->getValueVector(3).get()};

        std::unique_ptr<FactorizedTableSchema> tableSchema =
            std::make_unique<FactorizedTableSchema>();
        auto stringColumnSize =
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING});
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(
            false /* isUnflat */, 0 /* dataChunkPos */, stringColumnSize));
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(
            false /* isUnflat */, 0 /* dataChunkPos */, stringColumnSize));
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(
            false /* isUnflat */, 0 /* dataChunkPos */, stringColumnSize));
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(
            false /* isUnflat */, 0 /* dataChunkPos */, stringColumnSize));
        auto factorizedTable =
            std::make_unique<FactorizedTable>(memoryManager.get(), std::move(tableSchema));

        std::vector<bool> isAscOrder(strValues.size(), true);
        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, memoryManager.get(), factorizedTableIdx,
                numTuplesPerBlockInFT, OrderByKeyEncoder::getNumBytesPerTuple(orderByVectors));
        // Manually flatten the data chunk.
        dataChunk->state->selVector->resetSelectorToValuePosBuffer();
        dataChunk->state->selVector->selectedSize = 1;
        for (auto i = 0u; i < strValues[0].size(); i++) {
            dataChunk->state->selVector->selectedPositions[0] = i;
            factorizedTable->append(allVectors);
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }
        dataChunk->state->selVector->resetSelectorToUnselected();

        factorizedTables.emplace_back(std::move(factorizedTable));
        return orderByKeyEncoder;
    }
};

TEST_F(KeyBlockMergerTest, singleOrderByColInt64Test) {
    std::vector<int64_t> leftSortingData = {INT64_MIN, -8848, 1, 7, 13, INT64_MAX, 0 /* NULL */};
    std::vector<int64_t> rightSortingData = {INT64_MIN, -6, 4, 22, 32, 38, 0 /* NULL */};
    std::vector<bool> leftNullMasks = {false, false, false, false, false, false, true};
    std::vector<bool> rightNullMasks = {false, false, false, false, false, false, true};
    std::vector<uint64_t> expectedBlockOffsetOrder = {0, 0, 1, 1, 2, 2, 3, 4, 3, 4, 5, 5, 6, 6};
    std::vector<uint64_t> expectedFactorizedTableIdxOrder = {
        0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedBlockOffsetOrder, expectedFactorizedTableIdxOrder, LogicalTypeID::INT64,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColInt64NoNullTest) {
    std::vector<int64_t> leftSortingData = {INT64_MIN, -512, -5, 22, INT64_MAX};
    std::vector<int64_t> rightSortingData = {INT64_MIN, -999, 31, INT64_MAX};
    std::vector<bool> leftNullMasks(leftSortingData.size(), false);
    std::vector<bool> rightNullMasks(rightSortingData.size(), false);
    std::vector<uint64_t> expectedBlockOffsetOrder = {0, 0, 1, 1, 2, 3, 2, 4, 3};
    std::vector<uint64_t> expectedFactorizedTableIdxOrder = {0, 1, 1, 0, 0, 0, 1, 0, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedBlockOffsetOrder, expectedFactorizedTableIdxOrder, LogicalTypeID::INT64,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColInt64SameValueTest) {
    std::vector<int64_t> leftSortingData = {4, 4, 4, 4, 4, 4};
    std::vector<int64_t> rightSortingData = {4, 4, 4, 4, 4, 4, 4, 4, 4};
    std::vector<bool> leftNullMasks(leftSortingData.size(), false);
    std::vector<bool> rightNullMasks(rightSortingData.size(), false);
    std::vector<uint64_t> expectedBlockOffsetOrder = {0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<uint64_t> expectedFactorizedTableIdxOrder = {
        0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedBlockOffsetOrder, expectedFactorizedTableIdxOrder, LogicalTypeID::INT64,
        false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColInt64LargeNumTuplesTest) {
    std::vector<int64_t> leftSortingData, rightSortingData;
    std::vector<uint64_t> expectedBlockOffsetOrder(
        leftSortingData.size() + rightSortingData.size());
    std::vector<uint64_t> expectedFactorizedTableIdxOrder(
        leftSortingData.size() + rightSortingData.size());
    // Each memory block can hold a maximum of 240 tuples (4096 / (8 + 9)).
    // We fill the leftSortingData with the even numbers of 0-480 and the rightSortingData with
    // the odd numbers of 0-480 so that each of them takes up exactly one memoryBlock.
    for (auto i = 0u; i < 480; i++) {
        if (i % 2) {
            expectedBlockOffsetOrder.emplace_back(rightSortingData.size());
            expectedFactorizedTableIdxOrder.emplace_back(1);
            rightSortingData.emplace_back(i);
        } else {
            expectedBlockOffsetOrder.emplace_back(leftSortingData.size());
            expectedFactorizedTableIdxOrder.emplace_back(0);
            leftSortingData.emplace_back(i);
        }
    }
    std::vector<bool> leftNullMasks(leftSortingData.size(), false);
    std::vector<bool> rightNullMasks(rightSortingData.size(), false);
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedBlockOffsetOrder, expectedFactorizedTableIdxOrder, LogicalTypeID::INT64,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColStringTest) {
    std::vector<std::string> leftSortingData = {"" /* NULL */, "tiny str", "long std::string",
        "common prefix string3", "common prefix string1"};
    std::vector<std::string> rightSortingData = {"" /* NULL */, "" /* NULL */, "tiny str1",
        "common prefix string4", "common prefix string2", "common prefix string1",
        "" /* empty str */};
    std::vector<bool> leftNullMasks = {true, false, false, false, false};
    std::vector<bool> rightNullMasks = {true, true, false, false, false, false, false};
    std::vector<uint64_t> expectedBlockOffsetOrder = {0, 0, 1, 2, 1, 2, 3, 3, 4, 4, 5, 6};
    std::vector<uint64_t> expectedFactorizedTableIdxOrder = {0, 1, 1, 1, 0, 0, 1, 0, 1, 0, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedBlockOffsetOrder, expectedFactorizedTableIdxOrder, LogicalTypeID::STRING,
        false /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColStringNoNullTest) {
    std::vector<std::string> leftSortingData = {"common prefix string1", "common prefix string2",
        "common prefix string3", "long string", "tiny str"};
    std::vector<std::string> rightSortingData = {"common prefix string1", "common prefix string2",
        "common prefix string4", "tiny str", "tiny str1"};
    std::vector<bool> leftNullMasks(leftSortingData.size(), false);
    std::vector<bool> rightNullMasks(rightSortingData.size(), false);
    std::vector<uint64_t> expectedBlockOffsetOrder = {0, 0, 1, 1, 2, 2, 3, 4, 3, 4};
    std::vector<uint64_t> expectedFactorizedTableIdxOrder = {0, 1, 0, 1, 0, 1, 0, 0, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedBlockOffsetOrder, expectedFactorizedTableIdxOrder, LogicalTypeID::STRING,
        true /* isAsc */, false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColStringWithPayLoadTest) {
    std::vector<std::string> leftSortingData = {
        "", "", "abcabc str", "long long string1", "short str2"};
    std::vector<std::string> rightSortingData = {
        "", "test str1", "this is a long string", "very short", "" /* NULL */};
    std::vector<bool> leftNullMasks(leftSortingData.size(), false);
    std::vector<bool> rightNullMasks = {false, false, false, false, true};
    std::vector<uint64_t> expectedBlockOffsetOrder = {0, 1, 0, 2, 3, 4, 1, 2, 3, 4};
    std::vector<uint64_t> expectedFactorizedTableIdxOrder = {0, 0, 1, 0, 0, 0, 1, 1, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedBlockOffsetOrder, expectedFactorizedTableIdxOrder, LogicalTypeID::STRING,
        true /* isAsc */, true /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, multiple0rderByColNoStrTest) {
    multipleOrderByColTest(false /* hasStrCol */);
}

TEST_F(KeyBlockMergerTest, multiple0rderByColOneStrColTest) {
    multipleOrderByColTest(true /* hasStrCol */);
}

TEST_F(KeyBlockMergerTest, multipleStrKeyColsTest) {
    auto dataChunk1 = std::make_shared<DataChunk>(4);
    auto dataChunk2 = std::make_shared<DataChunk>(4);
    auto dataChunk3 = std::make_shared<DataChunk>(4);
    std::vector<std::vector<std::string>> strValues1 = {
        {"common str1", "common str1", "shorts1", "shorts2"},
        {"same str1", "same str1", "same str1", "same str1"},
        {"payload3", "payload1", "payload2", "payload4"},
        {"long long str4", "long long str6", "long long str3", "long long str2"}};
    std::vector<std::vector<std::string>> strValues2 = {{"common str1", "common str1", "shorts1"},
        {"same str1", "same str1", "same str1"}, {"payload3", "payload1", "payload2"},
        {
            "",
            "long long str5",
            "long long str4",
        }};

    std::vector<std::vector<std::string>> strValues3 = {{"common str1", "common str1"},
        {"same str1", "same str1"}, {"payload3", "payload1"}, {"largerStr", "long long str4"}};
    std::vector<std::shared_ptr<FactorizedTable>> factorizedTables;
    auto orderByKeyEncoder1 =
        prepareMultipleStrKeyColsEncoder(dataChunk1, strValues1, 0 /* ftIdx */, factorizedTables);
    auto orderByKeyEncoder2 =
        prepareMultipleStrKeyColsEncoder(dataChunk2, strValues2, 1 /* ftIdx */, factorizedTables);
    auto orderByKeyEncoder3 =
        prepareMultipleStrKeyColsEncoder(dataChunk3, strValues3, 2 /* ftIdx */, factorizedTables);

    std::vector<StrKeyColInfo> strKeyColsInfo = {
        StrKeyColInfo(factorizedTables[0]->getTableSchema()->getColOffset(0 /* colIdx */),
            0 /* colOffsetInEncodedKeyBlock */, true /* isAscOrder */),
        StrKeyColInfo(factorizedTables[0]->getTableSchema()->getColOffset(1 /* colIdx */),
            orderByKeyEncoder1.getEncodingSize(LogicalType(LogicalTypeID::STRING)),
            true /* isAscOrder */),
        StrKeyColInfo(factorizedTables[0]->getTableSchema()->getColOffset(3 /* colIdx */),
            orderByKeyEncoder1.getEncodingSize(LogicalType(LogicalTypeID::STRING)) * 2,
            true /* isAscOrder */)};

    KeyBlockMerger keyBlockMerger =
        KeyBlockMerger(factorizedTables, strKeyColsInfo, orderByKeyEncoder1.getNumBytesPerTuple());

    auto numBytesPerEntry = orderByKeyEncoder1.getNumBytesPerTuple();
    auto resultKeyBlock =
        std::make_shared<MergedKeyBlocks>(numBytesPerEntry, 7ul, memoryManager.get());
    auto keyBlockMergeTask = std::make_shared<KeyBlockMergeTask>(
        std::make_shared<MergedKeyBlocks>(numBytesPerEntry, orderByKeyEncoder1.getKeyBlocks()[0]),
        std::make_shared<MergedKeyBlocks>(numBytesPerEntry, orderByKeyEncoder2.getKeyBlocks()[0]),
        resultKeyBlock, keyBlockMerger);
    KeyBlockMergeMorsel keyBlockMergeMorsel(0, 4, 0, 3);
    keyBlockMergeMorsel.keyBlockMergeTask = keyBlockMergeTask;
    keyBlockMerger.mergeKeyBlocks(keyBlockMergeMorsel);

    auto resultMemBlock1 =
        std::make_shared<MergedKeyBlocks>(numBytesPerEntry, 9ul, memoryManager.get());
    auto keyBlockMergeTask1 = std::make_shared<KeyBlockMergeTask>(resultKeyBlock,
        std::make_shared<MergedKeyBlocks>(numBytesPerEntry, orderByKeyEncoder3.getKeyBlocks()[0]),
        resultMemBlock1, keyBlockMerger);
    KeyBlockMergeMorsel keyBlockMergeMorsel1(0, 7, 0, 2);
    keyBlockMergeMorsel1.keyBlockMergeTask = keyBlockMergeTask1;
    keyBlockMerger.mergeKeyBlocks(keyBlockMergeMorsel1);

    std::vector<uint64_t> expectedBlockOffsetOrder = {0, 0, 0, 1, 1, 1, 2, 2, 3};
    std::vector<uint64_t> expectedFactorizedTableIdxOrder = {1, 2, 0, 2, 1, 0, 0, 1, 0};
    checkTupleIdxesAndFactorizedTableIdxes(resultMemBlock1->getTuple(0),
        orderByKeyEncoder1.getNumBytesPerTuple(), expectedBlockOffsetOrder,
        expectedFactorizedTableIdxOrder);
}
