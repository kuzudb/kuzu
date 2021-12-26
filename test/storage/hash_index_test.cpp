#include <fstream>
#include <random>

#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/utils.h"
#include "src/storage/include/index/hash_index.h"

using namespace std;
using namespace graphflow::storage;
using namespace graphflow::common;

class LoadedHashIndexTest : public testing::Test {

public:
    LoadedHashIndexTest() : fName{TEMP_INDEX_DIR + "/0.index"} {
        dummyMetric = make_unique<NumericMetric>(false);
        bufferManagerMetrics =
            make_unique<BufferManagerMetrics>(*dummyMetric, *dummyMetric, *dummyMetric);
        FileUtils::createDir(TEMP_INDEX_DIR);
    }

    void TearDown() override { FileUtils::removeDir(TEMP_INDEX_DIR); }

public:
    const string TEMP_INDEX_DIR = "test/temp_index/";
    const string fName;
    uint64_t numKeysToInsert = 5000;

    unique_ptr<NumericMetric> dummyMetric;
    unique_ptr<BufferManagerMetrics> bufferManagerMetrics;
};

class LoadedHashIndexInt64KeyTest : public LoadedHashIndexTest {

public:
    void SetUp() override {
        HashIndex insertionHashIndex(fName, INT64);
        insertionHashIndex.bulkReserve(numKeysToInsert);
        // Inserting (key=i, value=i*2) pairs
        for (uint64_t k = 0; k < numKeysToInsert; k++) {
            insertionHashIndex.insert(reinterpret_cast<uint8_t*>(&k), k << 1);
        }
        insertionHashIndex.saveToDisk();
    }
};

class LoadedHashIndexStringKeyTest : public LoadedHashIndexTest {

public:
    LoadedHashIndexStringKeyTest() : LoadedHashIndexTest() {
        ifstream inf(inputFName, ios_base::in);
        inf.seekg(0, ios_base::end);
        auto numBlock = 1 + (inf.tellg() / CSV_READING_BLOCK_SIZE);
        inf.close();
        for (auto i = 0u; i < numBlock; i++) {
            CSVReader reader{inputFName, LoaderConfig::DEFAULT_TOKEN_SEPARATOR,
                LoaderConfig::DEFAULT_QUOTE_CHAR, LoaderConfig::DEFAULT_ESCAPE_CHAR, i};
            while (reader.hasNextLine()) {
                reader.hasNextToken();
                auto key = string(reader.getString());
                reader.hasNextToken();
                node_offset_t value = reader.getInt64();
                map[key] = value;
                reader.skipLine();
            }
        }
        assert(map.size() == numKeysToInsert);
    }

    void SetUp() override {
        HashIndex insertionHashIndex(fName, STRING);
        insertionHashIndex.bulkReserve(numKeysToInsert);
        for (auto& entry : map) {
            auto key = reinterpret_cast<uint8_t*>(const_cast<char*>(entry.first.c_str()));
            insertionHashIndex.insert(key, entry.second);
        }
        insertionHashIndex.saveToDisk();
    }

public:
    string inputFName = "dataset/hash-index-test/stringKeyNodeOffset.data";
    unordered_map<string, node_offset_t> map{};
};

TEST(HashIndexTest, HashIndexInt64KeyInsertExists) {
    HashIndex hashIndex("dummy_name", INT64);
    auto numEntries = 10;
    for (uint64_t i = 0; i < numEntries; i++) {
        ASSERT_TRUE(hashIndex.insert(reinterpret_cast<uint8_t*>(&i), i << 1));
    }
    for (auto i = 0; i < numEntries; i++) {
        ASSERT_FALSE(hashIndex.insert(reinterpret_cast<uint8_t*>(&i), i << 1));
    }
}

TEST(HashIndexTest, HashIndexStringKeyInsertExists) {
    HashIndex hashIndex("dummy_name", STRING);
    char const* strKeys[] = {"abc", "def", "ghi", "jkl", "mno"};
    for (uint64_t i = 0; i < 5; i++) {
        auto key = reinterpret_cast<uint8_t*>(const_cast<char*>(strKeys[i]));
        ASSERT_TRUE(hashIndex.insert(key, i));
    }
    for (auto i = 0; i < 5; i++) {
        auto key = reinterpret_cast<uint8_t*>(const_cast<char*>(strKeys[i]));
        ASSERT_FALSE(hashIndex.insert(key, i));
    }
}

TEST_F(LoadedHashIndexInt64KeyTest, HashIndexInt64SequentialLookupInMem) {
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

TEST_F(LoadedHashIndexInt64KeyTest, HashIndexInt64RandomLookupThroughBufferManager) {
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

TEST_F(LoadedHashIndexStringKeyTest, HashIndexInt64SequentialLookupInMem) {
    auto bufferManager = make_unique<BufferManager>(0);
    HashIndex hashIndex(fName, *bufferManager, true /*isInMemory*/);
    node_offset_t result;
    for (auto& entry : map) {
        auto key = reinterpret_cast<uint8_t*>(const_cast<char*>(entry.first.c_str()));
        auto found = hashIndex.lookup(key, result, *bufferManagerMetrics);
        ASSERT_TRUE(found);
        ASSERT_EQ(result, entry.second);
    }
}
