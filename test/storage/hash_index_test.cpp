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
    LoadedHashIndexTest() : fName{TEMP_INDEX_DIR + "0.index"} {
        FileUtils::createDir(TEMP_INDEX_DIR);
        writeBufferManager = make_unique<BufferManager>();
    }

    void TearDown() override { FileUtils::removeDir(TEMP_INDEX_DIR); }

public:
    unique_ptr<BufferManager> writeBufferManager;
    unique_ptr<MemoryManager> writeMemoryManager;
    const string TEMP_INDEX_DIR = "test/temp_index/";
    const string fName;
    uint64_t numKeysToInsert = 5000;
};

class LoadedHashIndexInt64KeyTest : public LoadedHashIndexTest {

public:
    void SetUp() override {
        HashIndex insertionHashIndex(
            fName, DataType(INT64), *writeBufferManager, false /* isInMemoryForLookup */);
        insertionHashIndex.bulkReserve(numKeysToInsert);
        // Inserting(key = i, value = i * 2) pairs
        for (uint64_t k = 0; k < numKeysToInsert; k++) {
            insertionHashIndex.insert(k, k << 1);
        }
        insertionHashIndex.flush();
    }
};

class LoadedHashIndexStringKeyTest : public LoadedHashIndexTest {

public:
    LoadedHashIndexStringKeyTest() : LoadedHashIndexTest() {
        ifstream inf(inputFName, ios_base::in);
        inf.seekg(0, ios_base::end);
        auto numBlock = 1 + (inf.tellg() / LoaderConfig::CSV_READING_BLOCK_SIZE);
        inf.close();
        for (auto i = 0u; i < numBlock; i++) {
            CSVReaderConfig config;
            CSVReader reader{inputFName, config, i};
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
        HashIndex insertionHashIndex(
            fName, DataType(STRING), *writeBufferManager, false /* isInMemoryForLookup */);
        insertionHashIndex.bulkReserve(numKeysToInsert);
        for (auto& entry : map) {
            insertionHashIndex.insert(entry.first.c_str(), entry.second);
        }
        insertionHashIndex.flush();
    }

public:
    string inputFName = "dataset/hash-index-test/stringKeyNodeOffset.data";
    unordered_map<string, node_offset_t> map{};
};

TEST(HashIndexTest, HashIndexInt64KeyInsertExists) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(
        "dummy_name_int", DataType(INT64), *bufferManager, false /* isInMemoryForLookup */);
    auto numEntries = 10;
    hashIndex.bulkReserve(10);
    for (uint64_t i = 0; i < numEntries; i++) {
        ASSERT_TRUE(hashIndex.insert(i, i << 1));
    }
    for (uint64_t i = 0; i < numEntries; i++) {
        ASSERT_FALSE(hashIndex.insert(i, i << 1));
    }
    hashIndex.flush();
}

TEST(HashIndexTest, HashIndexStringKeyInsertExists) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(
        "dummy_name_string", DataType(STRING), *bufferManager, false /* isInMemoryForLookup */);
    char const* strKeys[] = {"abc", "def", "ghi", "jkl", "mno"};
    hashIndex.bulkReserve(5);
    for (auto i = 0u; i < 5; i++) {
        ASSERT_TRUE(hashIndex.insert(strKeys[i], i));
    }
    for (auto i = 0u; i < 5; i++) {
        ASSERT_FALSE(hashIndex.insert(strKeys[i], i));
    }
    hashIndex.flush();
}

TEST_F(LoadedHashIndexInt64KeyTest, HashIndexInt64SequentialLookupInMem) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(fName, DataType(INT64), *bufferManager, true /*isInMemoryForLookup*/);
    node_offset_t result;
    for (uint64_t i = 0; i < numKeysToInsert; i++) {
        auto found = hashIndex.lookup(i, result);
        ASSERT_TRUE(found);
        ASSERT_EQ(result, i << 1);
    }
}

TEST_F(LoadedHashIndexInt64KeyTest, HashIndexInt64RandomLookupThroughBufferManager) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(fName, DataType(INT64), *bufferManager, true /*isInMemoryForLookup*/);
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
        hashIndex.lookup(key, result);
        ASSERT_EQ(result, key << 1);
    }
}

TEST_F(LoadedHashIndexStringKeyTest, HashIndexStringSequentialLookupInMem) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(fName, DataType(STRING), *bufferManager, true /*isInMemoryForLookup*/);
    node_offset_t result;
    for (auto& entry : map) {
        auto found = hashIndex.lookup(entry.first.c_str(), result);
        ASSERT_TRUE(found);
        ASSERT_EQ(result, entry.second);
    }
}

static void parallel_insert(HashIndex* index, int64_t startId, uint64_t num) {
    for (auto i = 0u; i < num; i++) {
        index->insert(startId + i, (startId + i) * 2);
    }
}

TEST(HashIndexTest, ParallelHashIndexInsertions) {
    auto bufferManager = make_unique<BufferManager>();
    auto hashIndex = make_unique<HashIndex>(
        "dummy", DataType(INT64), *bufferManager, false /* isInMemoryForLookup */);
    auto numKeysToInsert = 5000;
    hashIndex->bulkReserve(numKeysToInsert);
    auto numThreads = 10u;
    auto numKeysPerThread = numKeysToInsert / numThreads;
    thread threads[numThreads];
    auto startId = 0;
    for (auto i = 0u; i < numThreads; i++) {
        threads[i] = thread(parallel_insert, hashIndex.get(), startId, numKeysPerThread);
        startId += numKeysPerThread;
    }
    for (auto i = 0u; i < numThreads; i++) {
        threads[i].join();
    }
    hashIndex->flush();

    node_offset_t result;
    for (auto i = 0u; i < numKeysToInsert; i++) {
        ASSERT_TRUE(hashIndex->lookup(i, result));
        ASSERT_EQ(result, i * 2);
    }
}
