#include <iostream>
#include <random>

#include "gtest/gtest.h"
#include "spdlog/sinks/stdout_sinks.h"

#include "src/storage/include/index/hash_index.h"

using namespace std;
using namespace graphflow::storage;

class HashIndexTest : public testing::Test {
public:
    const string TEMP_INDEX = "test/temp_index/";

    void SetUp() override {
        if (!filesystem::exists(TEMP_INDEX) && !filesystem::create_directory(TEMP_INDEX)) {
            throw invalid_argument(
                "Graph output directory cannot be created. Check if it exists and remove it.");
        }
        spdlog::stdout_logger_mt("storage");
        MemoryManager insertionMemoryManager;
        BufferManager insertionBufferManager(DEFAULT_BUFFER_POOL_SIZE);
        OverflowPagesManager ovfPagesManager(TEMP_INDEX, insertionMemoryManager);
        HashIndex insertionHashIndex(
            TEMP_INDEX, 0, insertionMemoryManager, insertionBufferManager, ovfPagesManager, INT64);
        auto numEntries = 1000;
        auto state = make_shared<VectorState>(true, numEntries);
        state->size = numEntries;
        ValueVector keys(INT64);
        keys.state = state;
        ValueVector values(INT64);
        values.state = state;
        auto keysData = (uint64_t*)keys.values;
        auto valuesData = (uint64_t*)values.values;
        for (auto k = 0; k < numEntries; k++) {
            keysData[k] = k;
            valuesData[k] = keysData[k] * 2;
        }
        auto result = insertionHashIndex.insert(keys, values);
        insertionHashIndex.flush();
        spdlog::drop("buffer_manager");
    }

    void TearDown() override {
        error_code removeErrorCode;
        if (!filesystem::remove_all(TEMP_INDEX, removeErrorCode)) {
            spdlog::error("Remove graph output directory error: {}", removeErrorCode.message());
        }
        spdlog::drop("storage");
    }
};

TEST_F(HashIndexTest, HashIndexInsertExists) {
    MemoryManager memoryManager;
    BufferManager bufferManager(DEFAULT_BUFFER_POOL_SIZE);
    OverflowPagesManager ovfPagesManager(TEMP_INDEX, memoryManager);
    HashIndex hashIndex(TEMP_INDEX, 0, memoryManager, bufferManager, ovfPagesManager, INT64);
    auto numEntries = 10;
    auto state = make_shared<VectorState>(true, numEntries);
    state->size = numEntries;
    ValueVector keys(INT64);
    keys.state = state;
    ValueVector values(INT64);
    values.state = state;
    auto keysData = (uint64_t*)keys.values;
    auto valuesData = (uint64_t*)values.values;
    for (auto i = 0; i < numEntries; i++) {
        keysData[i] = i;
        valuesData[i] = keysData[i] * 2;
    }

    auto insertResult = hashIndex.insert(keys, values);
    for (auto i = 0; i < numEntries; i++) {
        ASSERT_FALSE(insertResult[i]);
    }
}

TEST_F(HashIndexTest, HashIndexSmallLookup) {
    MemoryManager memoryManager;
    BufferManager bufferManager(DEFAULT_BUFFER_POOL_SIZE);
    OverflowPagesManager ovfPagesManager(TEMP_INDEX, memoryManager);
    HashIndex lookupHashIndex(TEMP_INDEX, 0, memoryManager, bufferManager, ovfPagesManager, INT64);
    auto numEntries = 10;
    auto state = make_shared<VectorState>(true, numEntries);
    state->size = numEntries;
    ValueVector result(INT64);
    result.state = state;
    ValueVector keys(INT64);
    keys.state = state;
    auto keysData = (uint64_t*)keys.values;
    for (auto i = 0; i < numEntries; i++) {
        keysData[i] = i;
    }
    lookupHashIndex.lookup(keys, result);
    auto resultData = (uint64_t*)result.values;
    for (auto i = 0; i < numEntries; i++) {
        ASSERT_EQ(resultData[i], keysData[i] * 2);
    }
}

TEST_F(HashIndexTest, HashIndexRandomLookup) {
    MemoryManager memoryManager;
    BufferManager bufferManager(DEFAULT_BUFFER_POOL_SIZE);
    OverflowPagesManager ovfPagesManager(TEMP_INDEX, memoryManager);
    HashIndex lookupHashIndex(TEMP_INDEX, 0, memoryManager, bufferManager, ovfPagesManager, INT64);
    auto numEntries = 1000;
    auto state = make_shared<VectorState>(true, numEntries);
    state->size = numEntries;
    ValueVector keys(INT64);
    keys.state = state;
    auto keysData = (uint64_t*)keys.values;

    random_device rd;
    mt19937::result_type seed =
        rd() ^ ((mt19937::result_type)chrono::duration_cast<chrono::seconds>(
                    chrono::system_clock::now().time_since_epoch())
                       .count() +
                   (mt19937::result_type)chrono::duration_cast<chrono::microseconds>(
                       chrono::high_resolution_clock::now().time_since_epoch())
                       .count());
    mt19937 gen(seed);
    uniform_int_distribution<unsigned> distribution(0, numEntries - 1);

    for (auto i = 0; i < numEntries; i++) {
        keysData[i] = distribution(gen);
    }
    ValueVector result(INT64);
    result.state = state;
    lookupHashIndex.lookup(keys, result);
    auto resultData = (uint64_t*)result.values;
    for (auto i = 0; i < numEntries; i++) {
        ASSERT_EQ(resultData[i], keysData[i] * 2);
    }
}