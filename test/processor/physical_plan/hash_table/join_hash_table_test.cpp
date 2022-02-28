#include "include/gtest/gtest.h"

#include "src/processor/include/physical_plan/hash_table/join_hash_table.h"

using namespace graphflow::processor;

class HashTableTest : public ::testing::Test {

    void SetUp() override {
        bufferManager = make_unique<BufferManager>();
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        tableSchema.appendColumn({false /* isUnflat */, 0 /* dataChunkPos */, sizeof(int64_t)});
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    TableSchema tableSchema;
};

TEST_F(HashTableTest, HashTableInsertionAndLookupTest) {
    const auto numTuples = 5ul;

    JoinHashTable hashTable(*memoryManager, numTuples, tableSchema);
    int64_t values[numTuples] = {7, 20, 46, 3, 5};
    for (auto i = 0u; i < numTuples; i++) {
        hashTable.insertEntry<int64_t>((uint8_t*)(values + i));
    }
    for (auto i = 0u; i < numTuples; i++) {
        ASSERT_EQ(*(uint8_t**)hashTable.findHashEntry(values[i]), (uint8_t*)(values + i));
    }
}

TEST_F(HashTableTest, HashTableCollisionChainingTest) {
    const auto numTuples = 3ul;
    JoinHashTable hashTable(*memoryManager, numTuples, tableSchema);
    // The three sevens will cause a collision in the hashTable. After inserting all the values
    // to hashTable, the corresponding slot should only store a single pointer to the last 7.
    int64_t values[numTuples] = {7, 7, 7};
    for (auto i = 0u; i < numTuples; i++) {
        auto prevPtr = hashTable.insertEntry<int64_t>((uint8_t*)(values + i));
        ASSERT_EQ(prevPtr, i == 0 ? nullptr : (uint8_t*)(values + i - 1));
    }
    ASSERT_EQ(*(uint8_t**)hashTable.findHashEntry(values[1]), (uint8_t*)(values + 2));
}
