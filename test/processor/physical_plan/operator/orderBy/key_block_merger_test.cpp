#include <numeric>
#include <variant>
#include <vector>

#include "gtest/gtest.h"

#include "src/common/include/assert.h"
#include "src/common/include/configs.h"
#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/date.h"
#include "src/processor/include/physical_plan/operator/order_by/key_block_merger.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

using ::testing::Test;
using namespace graphflow::processor;
using namespace std;

class KeyBlockMergerTest : public Test {

public:
    void SetUp() override { memoryManager = make_unique<MemoryManager>(); }

public:
    unique_ptr<MemoryManager> memoryManager;

    void checkRowIDsAndThreadIDs(uint8_t* keyBlockPtr, const uint64_t keyBlockEntrySizeInBytes,
        const vector<uint64_t>& expectedRowIDOrder, const vector<uint64_t>& expectedThreadIDOrder) {
        assert(expectedRowIDOrder.size() == expectedThreadIDOrder.size());
        for (auto i = 0u; i < expectedRowIDOrder.size(); i++) {
            auto encodedRowID = OrderByKeyEncoder::getEncodedRowID(
                keyBlockPtr + keyBlockEntrySizeInBytes - sizeof(uint64_t));
            ASSERT_EQ(encodedRowID, expectedRowIDOrder[i]);
            ASSERT_EQ(OrderByKeyEncoder::getEncodedThreadID(
                          keyBlockPtr + keyBlockEntrySizeInBytes - sizeof(uint64_t)),
                expectedThreadIDOrder[i]);
            keyBlockPtr += keyBlockEntrySizeInBytes;
        }
    }

    template<typename T>
    OrderByKeyEncoder prepareSingleOrderByColEncoder(const vector<T>& sortingData,
        const vector<bool>& nullMasks, DataType dataType, bool isAsc, uint16_t threadID,
        bool hasPayLoadCol, vector<unique_ptr<RowCollection>>& rowCollections,
        shared_ptr<DataChunk>& dataChunk) {
        GF_ASSERT(sortingData.size() == nullMasks.size());
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

        vector<shared_ptr<ValueVector>> orderByVectors{
            valueVector}; // only contains orderBy columns
        vector<shared_ptr<ValueVector>> allVectors{
            valueVector}; // all columns including orderBy and payload columns

        RowLayout rowLayout;
        rowLayout.appendField(
            {dataType, TypeUtils::getDataTypeSize(dataType), false /* isVectorOverflow */});

        if (hasPayLoadCol) {
            auto payloadValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
            for (auto i = 0u; i < dataChunk->state->selectedSize; i++) {
                payloadValueVector->addString(i, to_string(i));
            }
            dataChunk->insert(1, payloadValueVector);
            // To test whether the orderByCol -> rowCollectionColOffset works properly, we put the
            // payload column at index 0, and the orderByCol at index 1. Then the
            // rowCollectionOffset for the key string column = sizeof(gf_string_t).
            allVectors.insert(allVectors.begin(), payloadValueVector);
            rowLayout.appendField({dataType, TypeUtils::getDataTypeSize(STRING), false});
        }
        rowLayout.initialize();

        auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
        rowCollection->append(allVectors, sortingData.size());

        vector<bool> isAscOrder = {isAsc};
        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager, threadID);
        orderByKeyEncoder.encodeKeys();

        rowCollections.emplace_back(move(rowCollection));
        return orderByKeyEncoder;
    }

    template<typename T>
    void singleOrderByColMergeTest(const vector<T>& leftSortingData,
        const vector<bool>& leftNullMasks, const vector<T>& rightSortingData,
        const vector<bool>& rightNullMasks, const vector<uint64_t>& expectedRowIDOrder,
        const vector<uint64_t>& expectedThreadIDOrder, const DataType dataType, const bool isAsc,
        bool hasPayLoadCol) {
        vector<unique_ptr<RowCollection>> rowCollections;
        auto dataChunk0 = make_shared<DataChunk>(hasPayLoadCol ? 2 : 1);
        auto dataChunk1 = make_shared<DataChunk>(hasPayLoadCol ? 2 : 1);
        auto orderByKeyEncoder1 = prepareSingleOrderByColEncoder(leftSortingData, leftNullMasks,
            dataType, isAsc, 0 /* threadID */, hasPayLoadCol, rowCollections, dataChunk0);
        auto orderByKeyEncoder2 = prepareSingleOrderByColEncoder(rightSortingData, rightNullMasks,
            dataType, isAsc, 1 /* threadID */, hasPayLoadCol, rowCollections, dataChunk1);

        vector<StrKeyColInfo> strKeyInfo;
        if (hasPayLoadCol) {
            strKeyInfo.emplace_back(StrKeyColInfo(TypeUtils::getDataTypeSize(STRING), 0, isAsc));
        } else if constexpr (is_same<T, string>::value) {
            strKeyInfo.emplace_back(StrKeyColInfo(0, 0, isAsc));
        }

        KeyBlockMerger keyBlockMerger =
            KeyBlockMerger(*memoryManager, orderByKeyEncoder1, rowCollections, strKeyInfo);
        KeyBlockMergeInfo leftKeyBlockMergeInfo(
            *orderByKeyEncoder1.getKeyBlocks()[0], 0, leftSortingData.size());
        KeyBlockMergeInfo rightKeyBlockMergeInfo(
            *orderByKeyEncoder2.getKeyBlocks()[0], 0, rightSortingData.size());

        auto resultMemBlock = memoryManager->allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE * 2);
        KeyBlockMergeInfo resultKeyBlockMergeInfo(
            *resultMemBlock, 0, leftSortingData.size() + rightSortingData.size());

        keyBlockMerger.mergeKeyBlocks(
            leftKeyBlockMergeInfo, rightKeyBlockMergeInfo, resultKeyBlockMergeInfo);

        checkRowIDsAndThreadIDs(resultMemBlock->data,
            orderByKeyEncoder1.getKeyBlockEntrySizeInBytes(), expectedRowIDOrder,
            expectedThreadIDOrder);
    }

    OrderByKeyEncoder prepareMultipleOrderByColsEncoder(uint16_t threadID,
        vector<unique_ptr<RowCollection>>& rowCollections, shared_ptr<DataChunk>& dataChunk,
        RowLayout& rowLayout) {
        vector<shared_ptr<ValueVector>> orderByVectors;
        for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
            orderByVectors.emplace_back(dataChunk->getValueVector(i));
        }

        vector<bool> isAscOrder(orderByVectors.size(), true);
        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager, threadID);

        auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
        for (auto i = 0u; i < dataChunk->state->selectedSize; i++) {
            rowCollection->append(orderByVectors, 1);
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }

        rowCollections.emplace_back(move(rowCollection));
        return orderByKeyEncoder;
    }

    void prepareMultipleOrderByColsValueVector(vector<int64_t>& int64Values,
        vector<double>& doubleValues, vector<timestamp_t>& timestampValues,
        shared_ptr<DataChunk>& dataChunk) {
        assert(int64Values.size() == doubleValues.size());
        assert(doubleValues.size() == timestampValues.size());
        dataChunk->state->selectedSize = int64Values.size();
        dataChunk->state->currIdx = 0;

        auto int64ValueVector = make_shared<ValueVector>(memoryManager.get(), INT64);
        auto doubleValueVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
        auto timestampValueVector = make_shared<ValueVector>(memoryManager.get(), TIMESTAMP);

        dataChunk->insert(0, int64ValueVector);
        dataChunk->insert(1, doubleValueVector);
        dataChunk->insert(2, timestampValueVector);

        for (auto i = 0u; i < int64Values.size(); i++) {
            ((int64_t*)int64ValueVector->values)[i] = int64Values[i];
            ((double*)doubleValueVector->values)[i] = doubleValues[i];
            ((timestamp_t*)timestampValueVector->values)[i] = timestampValues[i];
        }
    }

    void multipleOrderByColTest(bool hasStrCol) {
        vector<int64_t> int64Values1 = {INT64_MIN, -78, 23};
        vector<double> doubleValues1 = {3.28, -0.0001, 4.621};
        vector<timestamp_t> timestampValues1 = {
            Timestamp::FromCString("2035-07-01 11:14:33", strlen("2035-07-01 11:14:33")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123"))};
        auto dataChunk1 = make_shared<DataChunk>(3 + (hasStrCol ? 1 : 0));
        prepareMultipleOrderByColsValueVector(
            int64Values1, doubleValues1, timestampValues1, dataChunk1);

        vector<int64_t> int64Values2 = {INT64_MIN, -78, 23, INT64_MAX};
        vector<double> doubleValues2 = {0.58, -0.0001, 4.621, 4.621};
        vector<timestamp_t> timestampValues2 = {
            Timestamp::FromCString("2036-07-01 11:14:33", strlen("2036-07-01 11:14:33")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")),
            Timestamp::FromCString("1962-04-07 11:12:35.123", strlen("1962-04-07 11:12:35.123")),
            Timestamp::FromCString("2035-07-01 11:14:33", strlen("2035-07-01 11:14:33"))};
        auto dataChunk2 = make_shared<DataChunk>(3 + (hasStrCol ? 1 : 0));
        prepareMultipleOrderByColsValueVector(
            int64Values2, doubleValues2, timestampValues2, dataChunk2);

        RowLayout rowLayout;
        rowLayout.appendField(
            FieldInLayout(INT64, TypeUtils::getDataTypeSize(INT64), false /* isVectorOverflow */));
        rowLayout.appendField(FieldInLayout(
            DOUBLE, TypeUtils::getDataTypeSize(DOUBLE), false /* isVectorOverflow */));
        rowLayout.appendField(FieldInLayout(
            TIMESTAMP, TypeUtils::getDataTypeSize(TIMESTAMP), false /* isVectorOverflow */));

        if (hasStrCol) {
            rowLayout.appendField(FieldInLayout(
                STRING, TypeUtils::getDataTypeSize(STRING), false /* isVectorOverflow */));
            auto stringValueVector1 = make_shared<ValueVector>(memoryManager.get(), STRING);
            auto stringValueVector2 = make_shared<ValueVector>(memoryManager.get(), STRING);
            dataChunk1->insert(3, stringValueVector1);
            dataChunk2->insert(3, stringValueVector2);

            stringValueVector1->addString(0, "same prefix 123");
            stringValueVector1->addString(1, "same prefix 128");
            stringValueVector1->addString(2, "same prefix 123");

            stringValueVector2->addString(0, "same prefix 127");
            stringValueVector2->addString(1, "same prefix 123");
            stringValueVector2->addString(2, "same prefix 121");
            stringValueVector2->addString(3, "same prefix 126");
        }
        rowLayout.initialize();

        vector<unique_ptr<RowCollection>> rowCollections;
        for (auto i = 0; i < 4; i++) {
            rowCollections.emplace_back(make_unique<RowCollection>(*memoryManager, rowLayout));
        }
        auto orderByKeyEncoder2 = prepareMultipleOrderByColsEncoder(
            4 /* threadID */, rowCollections, dataChunk2, rowLayout);
        auto orderByKeyEncoder1 = prepareMultipleOrderByColsEncoder(
            5 /* threadID */, rowCollections, dataChunk1, rowLayout);

        vector<uint64_t> expectedRowIDOrder = {0, 0, 1, 1, 2, 2, 3};
        vector<uint64_t> expectedThreadIDOrder = {4, 5, 5, 4, 5, 4, 4};

        vector<StrKeyColInfo> strKeyInfo;
        if (hasStrCol) {
            strKeyInfo.emplace_back(StrKeyColInfo(rowCollections[0]->getFieldOffsetInRow(3),
                TypeUtils::getDataTypeSize(INT64) + TypeUtils::getDataTypeSize(DOUBLE) +
                    TypeUtils::getDataTypeSize(TIMESTAMP),
                true));
            expectedRowIDOrder = {0, 0, 1, 1, 2, 2, 3};
            expectedThreadIDOrder = {4, 5, 4, 5, 4, 5, 4};
        }

        KeyBlockMerger keyBlockMerger =
            KeyBlockMerger(*memoryManager, orderByKeyEncoder1, rowCollections, strKeyInfo);
        KeyBlockMergeInfo leftKeyBlockMergeInfo(*orderByKeyEncoder1.getKeyBlocks()[0], 0, 3);
        KeyBlockMergeInfo rightKeyBlockMergeInfo(*orderByKeyEncoder2.getKeyBlocks()[0], 0, 4);
        auto resultMemBlock = memoryManager->allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE * 2);
        KeyBlockMergeInfo resultKeyBlockMergeInfo(*resultMemBlock, 0, 7);

        keyBlockMerger.mergeKeyBlocks(
            leftKeyBlockMergeInfo, rightKeyBlockMergeInfo, resultKeyBlockMergeInfo);

        checkRowIDsAndThreadIDs(resultMemBlock->data,
            orderByKeyEncoder1.getKeyBlockEntrySizeInBytes(), expectedRowIDOrder,
            expectedThreadIDOrder);
    }

    OrderByKeyEncoder prepareMultipleStrKeyColsEncoder(shared_ptr<DataChunk>& dataChunk,
        vector<vector<string>>& strValues, uint16_t threadID,
        vector<unique_ptr<RowCollection>>& rowCollections) {
        dataChunk->state->currIdx = 0;
        dataChunk->state->selectedSize = strValues[0].size();
        for (auto i = 0u; i < strValues.size(); i++) {
            auto strValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
            dataChunk->insert(i, strValueVector);
            for (auto j = 0u; j < strValues[i].size(); j++) {
                strValueVector->addString(j, strValues[i][j]);
            }
        }

        // The first, second, fourth columns are keyColumns.
        vector<shared_ptr<ValueVector>> orderByVectors{dataChunk->getValueVector(0),
            dataChunk->getValueVector(1), dataChunk->getValueVector(3)};

        vector<shared_ptr<ValueVector>> allVectors{dataChunk->getValueVector(0),
            dataChunk->getValueVector(1), dataChunk->getValueVector(2),
            dataChunk->getValueVector(3)};

        RowLayout rowLayout(
            vector<FieldInLayout>(4, FieldInLayout(STRING, TypeUtils::getDataTypeSize(STRING),
                                         false /* isVectorOverflow */)));
        auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);

        vector<bool> isAscOrder(strValues.size(), true);
        auto orderByKeyEncoder =
            OrderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager, threadID);

        for (auto i = 0u; i < strValues[0].size(); i++) {
            rowCollection->append(allVectors, 1);
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }

        rowCollections.emplace_back(move(rowCollection));
        return orderByKeyEncoder;
    }
};

TEST_F(KeyBlockMergerTest, singleOrderByColInt64Test) {
    vector<int64_t> leftSortingData = {INT64_MIN, -8848, 1, 7, 13, INT64_MAX, 0 /* NULL */};
    vector<int64_t> rightSortingData = {INT64_MIN, -6, 4, 22, 32, 38, 0 /* NULL */};
    vector<bool> leftNullMasks = {false, false, false, false, false, false, true};
    vector<bool> rightNullMasks = {false, false, false, false, false, false, true};
    vector<uint64_t> expectedRowIDOrder = {0, 0, 1, 1, 2, 2, 3, 4, 3, 4, 5, 5, 6, 6};
    vector<uint64_t> expectedThreadIDOrder = {0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, expectedThreadIDOrder, INT64, true /* isAsc */,
        false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColInt64NoNullTest) {
    vector<int64_t> leftSortingData = {INT64_MIN, -512, -5, 22, INT64_MAX};
    vector<int64_t> rightSortingData = {INT64_MIN, -999, 31, INT64_MAX};
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    vector<uint64_t> expectedRowIDOrder = {0, 0, 1, 1, 2, 3, 2, 4, 3};
    vector<uint64_t> expectedThreadIDOrder = {0, 1, 1, 0, 0, 0, 1, 0, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, expectedThreadIDOrder, INT64, true /* isAsc */,
        false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColInt64SameValueTest) {
    vector<int64_t> leftSortingData = {4, 4, 4, 4, 4, 4};
    vector<int64_t> rightSortingData = {4, 4, 4, 4, 4, 4, 4, 4, 4};
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    vector<uint64_t> expectedRowIDOrder = {0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 7, 8};
    vector<uint64_t> expectedThreadIDOrder = {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, expectedThreadIDOrder, INT64, false /* isAsc */,
        false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColInt64LargeNumRowsTest) {
    vector<int64_t> leftSortingData, rightSortingData;
    vector<uint64_t> expectedRowIDOrder(leftSortingData.size() + rightSortingData.size());
    vector<uint64_t> expectedThreadIDOrder(leftSortingData.size() + rightSortingData.size());
    // Each memory block can hold a maximum of 240 rows (4096 / (8 + 9)).
    // We fill the leftSortingData with the even numbers of 0-480 and the rightSortingData with the
    // odd numbers of 0-480 so that each of them takes up exactly one memoryBlock.
    for (auto i = 0u; i < 480; i++) {
        if (i % 2) {
            expectedRowIDOrder.emplace_back(rightSortingData.size());
            expectedThreadIDOrder.emplace_back(1);
            rightSortingData.emplace_back(i);
        } else {
            expectedRowIDOrder.emplace_back(leftSortingData.size());
            expectedThreadIDOrder.emplace_back(0);
            leftSortingData.emplace_back(i);
        }
    }
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, expectedThreadIDOrder, INT64, true /* isAsc */,
        false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColStringTest) {
    vector<string> leftSortingData = {
        "" /* NULL */, "tiny str", "long string", "common prefix string3", "common prefix string1"};
    vector<string> rightSortingData = {"" /* NULL */, "" /* NULL */, "tiny str1",
        "common prefix string4", "common prefix string2", "common prefix string1",
        "" /* empty str */};
    vector<bool> leftNullMasks = {true, false, false, false, false};
    vector<bool> rightNullMasks = {true, true, false, false, false, false, false};
    vector<uint64_t> expectedRowIDOrder = {0, 0, 1, 2, 1, 2, 3, 3, 4, 4, 5, 6};
    vector<uint64_t> expectedThreadIDOrder = {0, 1, 1, 1, 0, 0, 1, 0, 1, 0, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, expectedThreadIDOrder, STRING, false /* isAsc */,
        false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColStringNoNullTest) {
    vector<string> leftSortingData = {"common prefix string1", "common prefix string2",
        "common prefix string3", "long string", "tiny str"};
    vector<string> rightSortingData = {"common prefix string1", "common prefix string2",
        "common prefix string4", "tiny str", "tiny str1"};
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    vector<uint64_t> expectedRowIDOrder = {0, 0, 1, 1, 2, 2, 3, 4, 3, 4};
    vector<uint64_t> expectedThreadIDOrder = {0, 1, 0, 1, 0, 1, 0, 0, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, expectedThreadIDOrder, STRING, true /* isAsc */,
        false /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, singleOrderByColStringWithPayLoadTest) {
    vector<string> leftSortingData = {"", "", "abcabc str", "long long string1", "short str2"};
    vector<string> rightSortingData = {
        "", "test str1", "this is a long string", "very short", "" /* NULL */};
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks = {false, false, false, false, true};
    vector<uint64_t> expectedRowIDOrder = {0, 1, 0, 2, 3, 4, 1, 2, 3, 4};
    vector<uint64_t> expectedThreadIDOrder = {0, 0, 1, 0, 0, 0, 1, 1, 1, 1};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, expectedThreadIDOrder, STRING, true /* isAsc */,
        true /* hasPayLoadCol */);
}

TEST_F(KeyBlockMergerTest, multiple0rderByColNoStrTest) {
    multipleOrderByColTest(false /* hasStrCol */);
}

TEST_F(KeyBlockMergerTest, multiple0rderByColOneStrColTest) {
    multipleOrderByColTest(true /* hasStrCol */);
}

TEST_F(KeyBlockMergerTest, multipleStrKeyColsTest) {
    auto dataChunk1 = make_shared<DataChunk>(4);
    auto dataChunk2 = make_shared<DataChunk>(4);
    auto dataChunk3 = make_shared<DataChunk>(4);
    vector<vector<string>> strValues1 = {{"common str1", "common str1", "shorts1", "shorts2"},
        {"same str1", "same str1", "same str1", "same str1"},
        {"payload3", "payload1", "payload2", "payload4"},
        {"long long str4", "long long str6", "long long str3", "long long str2"}};
    vector<vector<string>> strValues2 = {{"common str1", "common str1", "shorts1"},
        {"same str1", "same str1", "same str1"}, {"payload3", "payload1", "payload2"},
        {
            "",
            "long long str5",
            "long long str4",
        }};

    vector<vector<string>> strValues3 = {{"common str1", "common str1"}, {"same str1", "same str1"},
        {"payload3", "payload1"}, {"largerStr", "long long str4"}};
    vector<unique_ptr<RowCollection>> rowCollections;
    auto orderByKeyEncoder1 =
        prepareMultipleStrKeyColsEncoder(dataChunk1, strValues1, 0 /* threadID */, rowCollections);
    auto orderByKeyEncoder2 =
        prepareMultipleStrKeyColsEncoder(dataChunk2, strValues2, 1 /* threadID */, rowCollections);
    auto orderByKeyEncoder3 =
        prepareMultipleStrKeyColsEncoder(dataChunk3, strValues3, 2 /* threadID */, rowCollections);

    vector<StrKeyColInfo> strKeyInfo = {StrKeyColInfo(0, 0, true /* isAscOrder */),
        StrKeyColInfo(TypeUtils::getDataTypeSize(STRING),
            orderByKeyEncoder1.getEncodingSize(STRING), true /* isAscOrder */),
        StrKeyColInfo(TypeUtils::getDataTypeSize(STRING) * 3,
            orderByKeyEncoder1.getEncodingSize(STRING) * 2, true /* isAscOrder */)};

    KeyBlockMerger keyBlockMerger =
        KeyBlockMerger(*memoryManager, orderByKeyEncoder1, rowCollections, strKeyInfo);

    KeyBlockMergeInfo leftKeyBlockMergeInfo(*orderByKeyEncoder1.getKeyBlocks()[0], 0, 4);
    KeyBlockMergeInfo rightKeyBlockMergeInfo(*orderByKeyEncoder2.getKeyBlocks()[0], 0, 3);
    auto resultMemBlock = memoryManager->allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE * 2);
    KeyBlockMergeInfo resultKeyBlockMergeInfo(*resultMemBlock, 0, 7);
    keyBlockMerger.mergeKeyBlocks(
        leftKeyBlockMergeInfo, rightKeyBlockMergeInfo, resultKeyBlockMergeInfo);

    KeyBlockMergeInfo leftKeyBlockMergeInfo1 = KeyBlockMergeInfo(*resultMemBlock, 0, 7);
    KeyBlockMergeInfo rightKeyBlockMergeInfo1 =
        KeyBlockMergeInfo(*orderByKeyEncoder3.getKeyBlocks()[0], 0, 2);
    auto resultMemBlock1 = memoryManager->allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE * 3);
    KeyBlockMergeInfo resultKeyBlockMergeInfo1(*resultMemBlock1, 0, 9);
    keyBlockMerger.mergeKeyBlocks(
        leftKeyBlockMergeInfo1, rightKeyBlockMergeInfo1, resultKeyBlockMergeInfo1);

    vector<uint64_t> expectedRowIDOrder = {0, 0, 0, 1, 1, 1, 2, 2, 3};
    vector<uint64_t> expectedThreadIDOrder = {1, 2, 0, 2, 1, 0, 0, 1, 0};
    checkRowIDsAndThreadIDs(resultMemBlock1->data, orderByKeyEncoder1.getKeyBlockEntrySizeInBytes(),
        expectedRowIDOrder, expectedThreadIDOrder);
}
