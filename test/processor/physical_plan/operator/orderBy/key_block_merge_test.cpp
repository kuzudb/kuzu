#include <numeric>
#include <variant>
#include <vector>

#include "gtest/gtest.h"

#include "src/common/include/assert.h"
#include "src/common/include/configs.h"
#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/date.h"
#include "src/processor/include/physical_plan/operator/order_by/key_block_merge.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"
#include "src/processor/include/physical_plan/operator/order_by/radix_sort.h"

using ::testing::Test;
using namespace graphflow::processor;
using namespace std;

class KeyBlockMergeTest : public Test {

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
    void singleOrderByColMergeTest(const vector<T>& leftSortingData,
        const vector<bool>& leftNullMasks, const vector<T>& rightSortingData,
        const vector<bool>& rightNullMasks, const vector<uint64_t>& expectedRowIDOrder,
        const DataType dataType, const bool isAsc) {
        GF_ASSERT(leftSortingData.size() == leftNullMasks.size());
        GF_ASSERT(rightSortingData.size() == rightNullMasks.size());
        GF_ASSERT((leftSortingData.size() + rightSortingData.size()) == expectedRowIDOrder.size());

        auto dataChunk = make_shared<DataChunk>(1);
        dataChunk->state->currIdx = 0;
        auto valueVector = make_shared<ValueVector>(memoryManager.get(), dataType);
        dataChunk->insert(0, valueVector);
        vector<shared_ptr<ValueVector>> orderByVectors{valueVector};
        vector<bool> isAscOrder{isAsc};
        auto values = (T*)valueVector->values;
        auto orderByKeyEncoder = OrderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager);

        for (auto i = 0u; i < leftSortingData.size(); i++) {
            if (leftNullMasks[i]) {
                valueVector->setNull(i, true);
            } else if constexpr (is_same<T, string>::value) {
                valueVector->addString(i, leftSortingData[i]);
            } else {
                values[i] = leftSortingData[i];
            }
        }

        for (auto i = 0u; i < leftSortingData.size(); i++) {
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }

        // we can mark the current block as full, so the encoder will put the binary strings in a
        // new memory block.
        orderByKeyEncoder.curBlockUsedEntries =
            SORT_BLOCK_SIZE / OrderByKeyEncoder::getEntrySize(orderByVectors);
        for (auto i = 0u; i < rightSortingData.size(); i++) {
            if (rightNullMasks[i]) {
                valueVector->setNull(leftSortingData.size() + i, true);
            } else if constexpr (is_same<T, string>::value) {
                valueVector->addString(leftSortingData.size() + i, rightSortingData[i]);
            } else {
                values[leftSortingData.size() + i] = rightSortingData[i];
            }
        }

        for (auto i = 0u; i < rightSortingData.size(); i++) {
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }

        RadixSort radixSort = RadixSort(orderByVectors, *memoryManager, isAscOrder);
        radixSort.sortKeyBlock(*orderByKeyEncoder.getKeyBlocks()[0], leftSortingData.size());
        radixSort.sortKeyBlock(*orderByKeyEncoder.getKeyBlocks()[1], rightSortingData.size());

        KeyBlockMerger keyBlockMerge = KeyBlockMerger(orderByVectors, isAscOrder, *memoryManager);
        vector<pair<uint64_t, uint64_t>> KeyBlockMergeIdx = {
            make_pair(0, leftSortingData.size() - 1), make_pair(0, rightSortingData.size() - 1)};
        auto mergedKeyBlock =
            keyBlockMerge.mergeKeyBlocks(orderByKeyEncoder.getKeyBlocks(), KeyBlockMergeIdx);
        checkRowIDs(mergedKeyBlock->data, OrderByKeyEncoder::getEntrySize(orderByVectors),
            expectedRowIDOrder);
    }

    void multipleOrderByColTest(bool hasStrCol) {
        vector<uint64_t> expectedRowIDOrder = {1, 5, 2, 6, 0, 3, 4};
        auto dataChunk = make_shared<DataChunk>(2 + (hasStrCol ? 1 : 0));
        dataChunk->state->currIdx = 0;
        auto int64ValueVector = make_shared<ValueVector>(memoryManager.get(), INT64);
        auto doubleValueVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);

        dataChunk->insert(0, int64ValueVector);
        dataChunk->insert(1, doubleValueVector);

        vector<shared_ptr<ValueVector>> orderByVectors{int64ValueVector, doubleValueVector};
        vector<bool> isAscOrder = {true, false};
        auto int64Values = (int64_t*)int64ValueVector->values;
        auto doubleValues = (double*)doubleValueVector->values;

        int64Values[0] = 23;
        int64Values[1] = INT64_MIN;
        int64Values[2] = -78;
        int64Values[3] = 23;
        int64Values[4] = INT64_MAX;
        int64Values[5] = INT64_MIN;
        int64Values[6] = -78;

        doubleValues[0] = 4.621;
        doubleValueVector->setNull(1, true);
        doubleValues[2] = -0.0001;
        doubleValues[3] = -0.0001;
        doubleValues[4] = 4.621;
        doubleValues[5] = 0.58;
        doubleValues[6] = -0.0001;

        if (hasStrCol) {
            auto stringValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
            dataChunk->insert(2, stringValueVector);
            orderByVectors.emplace_back(stringValueVector);
            isAscOrder.emplace_back(true);
            stringValueVector->addString(0, "same prefix 123");
            stringValueVector->addString(1, "same prefix 123");
            stringValueVector->addString(2, "same prefix 128");
            stringValueVector->addString(3, "same prefix 125");
            stringValueVector->addString(4, "same prefix 126");
            stringValueVector->addString(5, "same prefix 127");
            stringValueVector->addString(6, "same prefix 123");
            expectedRowIDOrder = {1, 5, 6, 2, 0, 3, 4};
        }

        OrderByKeyEncoder orderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager);
        for (auto i = 0u; i < 3; i++) {
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }

        // we can mark the current block as full, so the encoder will put the binary strings in a
        // new memory block.
        orderByKeyEncoder.curBlockUsedEntries =
            SORT_BLOCK_SIZE / OrderByKeyEncoder::getEntrySize(orderByVectors);

        for (auto i = 0u; i < 4; i++) {
            orderByKeyEncoder.encodeKeys();
            dataChunk->state->currIdx++;
        }

        RadixSort radixSort = RadixSort(orderByVectors, *memoryManager, isAscOrder);
        radixSort.sortKeyBlock(*orderByKeyEncoder.getKeyBlocks()[0], 3);
        radixSort.sortKeyBlock(*orderByKeyEncoder.getKeyBlocks()[1], 4);

        KeyBlockMerger keyBlockMerge = KeyBlockMerger(orderByVectors, isAscOrder, *memoryManager);
        vector<pair<uint64_t, uint64_t>> KeyBlockMergeIdx = {make_pair(0, 2), make_pair(0, 3)};
        auto mergedKeyBlock =
            keyBlockMerge.mergeKeyBlocks(orderByKeyEncoder.getKeyBlocks(), KeyBlockMergeIdx);
        checkRowIDs(mergedKeyBlock->data, OrderByKeyEncoder::getEntrySize(orderByVectors),
            expectedRowIDOrder);
    }
};

TEST_F(KeyBlockMergeTest, singleOrderByColInt64Test) {
    vector<int64_t> leftSortingData = {1, 0 /*NULL*/, 13, 7, INT64_MAX, INT64_MIN, -8848};
    vector<int64_t> rightSortingData = {INT64_MIN, 4, -6, 22, 32, 38, 0 /*NULL*/};
    vector<bool> leftNullMasks = {false, true, false, false, false, false, false};
    vector<bool> rightNullMasks = {false, false, false, false, false, false, true};
    vector<uint64_t> expectedRowIDOrder = {5, 7, 6, 9, 0, 8, 3, 2, 10, 11, 12, 4, 1, 13};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, INT64, true);
}

TEST_F(KeyBlockMergeTest, singleOrderByColInt64NoNullTest) {
    vector<int64_t> leftSortingData = {-5, INT64_MIN, 22, INT64_MAX, -512};
    vector<int64_t> rightSortingData = {INT64_MAX, 31, INT64_MIN, -999};
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    vector<uint64_t> expectedRowIDOrder = {3, 5, 6, 2, 0, 4, 8, 1, 7};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, INT64, false);
}

TEST_F(KeyBlockMergeTest, singleOrderByColInt64SameValueTest) {
    vector<int64_t> leftSortingData = {4, 4, 4, 4, 4, 4};
    vector<int64_t> rightSortingData = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4};
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    vector<uint64_t> expectedRowIDOrder(leftSortingData.size() + rightSortingData.size(), -1);
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, INT64, false);
}

TEST_F(KeyBlockMergeTest, singleOrderByColInt64LargeNumRowsTest) {
    vector<int64_t> leftSortingData, rightSortingData;
    vector<uint64_t> expectedRowIDOrder(leftSortingData.size() + rightSortingData.size());
    // each memory block can hold a maximum of 240 rows (4096 / (8 + 9))
    // we fill the leftSortingData with the even numbers of 0-480 and the rightSortingData with the
    // odd numbers of 0-480 so that each of them takes up exactly one memoryBlock.
    for (auto i = 0u; i < 480; i++) {
        if (i % 2) {
            expectedRowIDOrder.emplace_back(rightSortingData.size() + 240);
            rightSortingData.emplace_back(i);
        } else {
            expectedRowIDOrder.emplace_back(leftSortingData.size());
            leftSortingData.emplace_back(i);
        }
    }
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, INT64, true);
}

TEST_F(KeyBlockMergeTest, singleOrderByColStringTest) {
    vector<string> leftSortingData = {
        "tiny str", "common prefix string1", "common prefix string3", "" /*NULL*/, "long string"};
    vector<string> rightSortingData = {"tiny str1", "" /*NULL*/, "common prefix string1",
        "common prefix string4", "common prefix string2", "" /*NULL*/, "" /*empty str*/,
        "" /*empty str*/};
    vector<bool> leftNullMasks = {false, false, false, true, false};
    vector<bool> rightNullMasks = {false, true, false, false, false, true, false, false};
    vector<uint64_t> expectedRowIDOrder = {6, 10, 3, 5, 0, 4, 8, 2, 9, 7, 1, 11, 12};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, STRING, false);
}

TEST_F(KeyBlockMergeTest, singleOrderByColStringNoNullTest) {
    vector<string> leftSortingData = {"tiny str", "common prefix string1", "common prefix string3",
        "common prefix string2", "long string"};
    vector<string> rightSortingData = {"tiny str1", "tiny str", "common prefix string1",
        "common prefix string4", "common prefix string2"};
    vector<bool> leftNullMasks(leftSortingData.size(), false);
    vector<bool> rightNullMasks(rightSortingData.size(), false);
    vector<uint64_t> expectedRowIDOrder = {7, 1, 9, 3, 2, 8, 4, 6, 0, 5};
    singleOrderByColMergeTest(leftSortingData, leftNullMasks, rightSortingData, rightNullMasks,
        expectedRowIDOrder, STRING, true);
}

TEST_F(KeyBlockMergeTest, multiple0rderByColNoStrTest) {
    multipleOrderByColTest(false);
}

TEST_F(KeyBlockMergeTest, multiple0rderByColOneStrColTest) {
    multipleOrderByColTest(true);
}

TEST_F(KeyBlockMergeTest, multipleStrColTest) {
    auto dataChunk = make_shared<DataChunk>(4);
    dataChunk->state->currIdx = 0;
    auto int64ValueVector1 = make_shared<ValueVector>(memoryManager.get(), INT64);
    auto int64ValueVector2 = make_shared<ValueVector>(memoryManager.get(), INT64);
    auto stringValueVector1 = make_shared<ValueVector>(memoryManager.get(), STRING);
    auto stringValueVector2 = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(0, int64ValueVector1);
    dataChunk->insert(1, stringValueVector1);
    dataChunk->insert(2, int64ValueVector2);
    dataChunk->insert(3, stringValueVector2);

    vector<shared_ptr<ValueVector>> orderByVectors{
        int64ValueVector1, stringValueVector1, int64ValueVector2, stringValueVector2};
    vector<bool> isAscOrder = {true, false, false, true};
    vector<uint64_t> expectedRowIDOrder = {0, 4, 3, 1, 2};

    auto int64Values1 = (int64_t*)int64ValueVector1->values;
    auto int64Values2 = (int64_t*)int64ValueVector2->values;

    int64Values1[0] = 23;
    int64Values1[1] = 23;
    int64ValueVector1->setNull(2, true);
    int64Values1[3] = 23;
    int64Values1[4] = 23;

    int64Values2[0] = 39;
    int64Values2[1] = 35;
    int64Values2[2] = -78;
    int64Values2[3] = 35;
    int64Values2[4] = 39;

    stringValueVector1->addString(0, "same common prefix");
    stringValueVector1->addString(0, "same common prefix");
    stringValueVector1->addString(0, "same common prefix");
    stringValueVector1->addString(0, "same common prefix");
    stringValueVector1->addString(0, "same common prefix");

    stringValueVector2->addString(0, "common prefix1.1");
    stringValueVector2->addString(0, "common prefix2.4");
    stringValueVector2->addString(0, "random str");
    stringValueVector2->addString(0, "common prefix2.3");
    stringValueVector2->addString(0, "common prefix1.5");

    OrderByKeyEncoder orderByKeyEncoder(orderByVectors, isAscOrder, *memoryManager);
    for (auto i = 0u; i < 3; i++) {
        orderByKeyEncoder.encodeKeys();
        dataChunk->state->currIdx++;
    }

    // we can mark the current block as full, so the encoder will put the binary strings in a new
    // memory block.
    orderByKeyEncoder.curBlockUsedEntries =
        SORT_BLOCK_SIZE / OrderByKeyEncoder::getEntrySize(orderByVectors);

    for (auto i = 0u; i < 2; i++) {
        orderByKeyEncoder.encodeKeys();
        dataChunk->state->currIdx++;
    }

    RadixSort radixSort = RadixSort(orderByVectors, *memoryManager, isAscOrder);
    radixSort.sortKeyBlock(*orderByKeyEncoder.getKeyBlocks()[0], 3);
    radixSort.sortKeyBlock(*orderByKeyEncoder.getKeyBlocks()[1], 2);

    KeyBlockMerger keyBlockMerge = KeyBlockMerger(orderByVectors, isAscOrder, *memoryManager);
    vector<pair<uint64_t, uint64_t>> KeyBlockMergeIdx = {make_pair(0, 2), make_pair(0, 1)};
    auto mergedKeyBlock =
        keyBlockMerge.mergeKeyBlocks(orderByKeyEncoder.getKeyBlocks(), KeyBlockMergeIdx);
    checkRowIDs(
        mergedKeyBlock->data, OrderByKeyEncoder::getEntrySize(orderByVectors), expectedRowIDOrder);
}
