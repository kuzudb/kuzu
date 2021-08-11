#include <random>

#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/utils.h"
#include "src/storage/include/index/hash_index.h"

using namespace std;
using namespace graphflow::storage;
using namespace graphflow::testing;

class HashIndexTest : public testing::Test {
public:
    const string TEMP_INDEX = "test/temp_index/";

    void SetUp() override {
        FileUtils::createDir(TEMP_INDEX);
        LoggerUtils::getOrCreateSpdLogger("storage");
        auto insertionMemoryManager = make_unique<MemoryManager>();
        auto insertionBufferManager =
            make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE);
        OverflowPagesManager ovfPagesManager(TEMP_INDEX, *insertionMemoryManager);
        HashIndex insertionHashIndex(TEMP_INDEX, 0, *insertionMemoryManager,
            *insertionBufferManager, ovfPagesManager, INT64);
        auto numEntries = 1000;
        auto state = make_shared<DataChunkState>(numEntries);
        state->selectedSize = numEntries;
        ValueVector keys(insertionMemoryManager.get(), INT64);
        keys.state = state;
        ValueVector values(insertionMemoryManager.get(), INT64);
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
        dummyMetric = make_unique<NumericMetric>(false);
        bufferManagerMetrics =
            make_unique<BufferManagerMetrics>(*dummyMetric, *dummyMetric, *dummyMetric);
    }

    void TearDown() override {
        FileUtils::removeDir(TEMP_INDEX);
        spdlog::drop("storage");
    }

public:
    unique_ptr<NumericMetric> dummyMetric;
    unique_ptr<BufferManagerMetrics> bufferManagerMetrics;
};

TEST_F(HashIndexTest, HashIndexInsertExists) {
    auto memoryManager = make_unique<MemoryManager>();
    auto bufferManager = make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE);
    OverflowPagesManager ovfPagesManager(TEMP_INDEX, *memoryManager);
    HashIndex hashIndex(TEMP_INDEX, 0, *memoryManager, *bufferManager, ovfPagesManager, INT64);
    auto numEntries = 10;
    auto state = make_shared<DataChunkState>(numEntries);
    state->selectedSize = numEntries;
    ValueVector keys(memoryManager.get(), INT64);
    keys.state = state;
    ValueVector values(memoryManager.get(), INT64);
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
    auto memoryManager = make_unique<MemoryManager>();
    auto bufferManager = make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE);
    OverflowPagesManager ovfPagesManager(TEMP_INDEX, *memoryManager);
    HashIndex hashIndex(TEMP_INDEX, 0, *memoryManager, *bufferManager, ovfPagesManager, INT64);
    auto numEntries = 10;
    auto state = make_shared<DataChunkState>(numEntries);
    state->selectedSize = numEntries;
    ValueVector result(memoryManager.get(), INT64);
    result.state = state;
    ValueVector keys(memoryManager.get(), INT64);
    keys.state = state;
    auto keysData = (uint64_t*)keys.values;
    for (auto i = 0; i < numEntries; i++) {
        keysData[i] = i;
    }
    hashIndex.lookup(keys, result, *bufferManagerMetrics);
    auto resultData = (uint64_t*)result.values;
    for (auto i = 0; i < numEntries; i++) {
        ASSERT_EQ(resultData[i], keysData[i] * 2);
    }
}

TEST_F(HashIndexTest, HashIndexRandomLookup) {
    auto memoryManager = make_unique<MemoryManager>();
    auto bufferManager = make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE);
    OverflowPagesManager ovfPagesManager(TEMP_INDEX, *memoryManager);
    HashIndex hashIndex(TEMP_INDEX, 0, *memoryManager, *bufferManager, ovfPagesManager, INT64);
    auto numEntries = 1000;
    auto state = make_shared<DataChunkState>(numEntries);
    state->selectedSize = numEntries;
    ValueVector keys(memoryManager.get(), INT64);
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
    ValueVector result(memoryManager.get(), INT64);
    result.state = state;
    hashIndex.lookup(keys, result, *bufferManagerMetrics);
    auto resultData = (uint64_t*)result.values;
    for (auto i = 0; i < numEntries; i++) {
        ASSERT_EQ(resultData[i], keysData[i] * 2);
    }
}