#include <random>

#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/utils.h"
#include "src/storage/include/index/hash_index.h"

using namespace std;
using namespace graphflow::storage;
using namespace graphflow::testing;

class LoadedHashIndexTest : public testing::Test {
public:
    LoadedHashIndexTest() : fName{TEMP_INDEX_DIR + "/0.index"} {
        dummyMetric = make_unique<NumericMetric>(false);
        bufferManagerMetrics =
            make_unique<BufferManagerMetrics>(*dummyMetric, *dummyMetric, *dummyMetric);
        FileUtils::createDir(TEMP_INDEX_DIR);
    }

    void SetUp() override {
        HashIndex insertionHashIndex(INT64);
        insertionHashIndex.bulkReserve(numKeysToInsert);
        for (uint64_t k = 0; k < numKeysToInsert; k++) {
            insertionHashIndex.insert(reinterpret_cast<uint8_t*>(&k), k << 1);
        }
        insertionHashIndex.saveToDisk(fName);
    }

    void TearDown() override { FileUtils::removeDir(TEMP_INDEX_DIR); }

public:
    const string TEMP_INDEX_DIR = "test/temp_index/";
    const string fName;
    uint64_t numKeysToInsert = 5000;

    unique_ptr<NumericMetric> dummyMetric;
    unique_ptr<BufferManagerMetrics> bufferManagerMetrics;
};

TEST(HashIndexTest, HashIndexInsertExists) {
    HashIndex hashIndex(INT64);
    auto numEntries = 10;
    for (uint64_t i = 0; i < numEntries; i++) {
        ASSERT_TRUE(hashIndex.insert(reinterpret_cast<uint8_t*>(&i), i << 1));
    }
    for (auto i = 0; i < numEntries; i++) {
        ASSERT_FALSE(hashIndex.insert(reinterpret_cast<uint8_t*>(&i), i << 1));
    }
}

TEST_F(LoadedHashIndexTest, HashIndexSequentialLookupInMem) {
    auto bufferManager = make_unique<BufferManager>(0);
    HashIndex hashIndex(fName, *bufferManager, true /*isInMemory*/);
    node_offset_t result;
    for (uint64_t i = 0; i < numKeysToInsert; i++) {
        auto found =
            hashIndex.lookup(reinterpret_cast<uint8_t*>(&i), result, *bufferManagerMetrics);
        ASSERT_TRUE(found);
        ASSERT_EQ(result, i << 1);
    }
}

TEST_F(LoadedHashIndexTest, HashIndexRandomLookupThroughBufferManager) {
    auto bufferManager = make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE);
    HashIndex hashIndex(fName, *bufferManager, false /*isInMemory*/);
    random_device rd;
    mt19937::result_type seed =
        rd() ^ ((mt19937::result_type)chrono::duration_cast<chrono::seconds>(
                    chrono::system_clock::now().time_since_epoch())
                       .count() +
                   (mt19937::result_type)chrono::duration_cast<chrono::microseconds>(
                       chrono::high_resolution_clock::now().time_since_epoch())
                       .count());
    mt19937 gen(seed);
    uniform_int_distribution<unsigned> distribution(0, numKeysToInsert - 1);
    node_offset_t result;
    for (auto i = 0; i < 10000; i++) {
        uint64_t key = distribution(gen);
        hashIndex.lookup(reinterpret_cast<uint8_t*>(&key), result, *bufferManagerMetrics);
        ASSERT_EQ(result, key << 1);
    }
}
