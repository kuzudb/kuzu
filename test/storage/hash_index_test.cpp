#include <random>

#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/utils.h"
#include "src/loader/in_mem_index/include/in_mem_hash_index.h"
#include "src/storage/index/include/hash_index.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::loader;
using namespace graphflow::storage;

class HashIndexTest : public testing::Test {

public:
    HashIndexTest()
        : storageStructureIdAndFName{StorageUtils::getNodeIndexIDAndFName(
              "" /* dummy directory */, -1 /* dummy node label */)} {
        storageStructureIdAndFName.fName = TEMP_INDEX_DIR + "dummy_ints.hindex";
        FileUtils::createDir(TEMP_INDEX_DIR);
    }

    void TearDown() override { FileUtils::removeDir(TEMP_INDEX_DIR); }

public:
    const string TEMP_INDEX_DIR = "test/temp_index/";
    StorageStructureIDAndFName storageStructureIdAndFName;
    uint64_t numKeysToInsert = 5000;
};

class LoadedHashIndexInt64KeyTest : public HashIndexTest {

public:
    LoadedHashIndexInt64KeyTest() : HashIndexTest() {}

    void SetUp() override {
        InMemHashIndexBuilder hashIndexBuilder(storageStructureIdAndFName.fName, DataType(INT64));
        hashIndexBuilder.bulkReserve(numKeysToInsert);
        // Inserting(key = i, value = i * 2) pairs
        for (int64_t k = 0; k < numKeysToInsert; k++) {
            hashIndexBuilder.append(k, k << 1);
        }
        hashIndexBuilder.flush();
    }
};

class LoadedHashIndexStringKeyTest : public HashIndexTest {

public:
    LoadedHashIndexStringKeyTest() : HashIndexTest() {}

    void SetUp() override {
        storageStructureIdAndFName.fName = TEMP_INDEX_DIR + "dummy_strings.hindex";
        InMemHashIndexBuilder hashIndexBuilder(storageStructureIdAndFName.fName, DataType(STRING));
        hashIndexBuilder.bulkReserve(numKeysToInsert);
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
                hashIndexBuilder.append(key.c_str(), value);
                reader.skipLine();
            }
        }
        hashIndexBuilder.flush();
    }

public:
    string inputFName = "dataset/hash-index-test/stringKeyNodeOffset.data";
    unordered_map<string, node_offset_t> map{};
};

TEST_F(LoadedHashIndexInt64KeyTest, HashIndexInt64SequentialLookup) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(
        storageStructureIdAndFName, DataType(INT64), *bufferManager, true /*isInMemory*/);
    node_offset_t result;
    for (int64_t i = 0; i < numKeysToInsert; i++) {
        auto found = hashIndex.lookup(i, result);
        ASSERT_TRUE(found);
        ASSERT_EQ(result, i << 1);
    }
}

TEST_F(LoadedHashIndexInt64KeyTest, HashIndexInt64RandomLookup) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(
        storageStructureIdAndFName, DataType(INT64), *bufferManager, true /*isInMemory*/);
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
    for (auto i = 0u; i < 10000; i++) {
        int64_t key = distribution(gen);
        hashIndex.lookup(key, result);
        ASSERT_EQ(result, key << 1);
    }
}

TEST_F(LoadedHashIndexStringKeyTest, HashIndexStringSequentialLookup) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(
        storageStructureIdAndFName, DataType(STRING), *bufferManager, true /*isInMemory*/);
    node_offset_t result;
    for (auto& entry : map) {
        auto found = hashIndex.lookup(entry.first.c_str(), result);
        ASSERT_TRUE(found);
        ASSERT_EQ(result, entry.second);
    }
}

TEST_F(LoadedHashIndexInt64KeyTest, HashIndexDeleteAndLookup) {
    auto bufferManager = make_unique<BufferManager>();
    HashIndex hashIndex(
        storageStructureIdAndFName, DataType(INT64), *bufferManager, true /*isInMemory*/);
    for (int64_t i = 0; i < numKeysToInsert; i++) {
        if (i % 10 == 0) {
            hashIndex.deleteKey(i);
        }
    }
    node_offset_t result;
    for (int64_t i = 0; i < numKeysToInsert; i++) {
        auto found = hashIndex.lookup(i, result);
        if (i % 10 == 0) {
            ASSERT_FALSE(found);
        } else {
            ASSERT_TRUE(found);
            ASSERT_EQ(result, i << 1);
        }
    }
}

TEST_F(HashIndexTest, HashIndexInt64KeyInsertExists) {
    auto bufferManager = make_unique<BufferManager>();
    InMemHashIndexBuilder hashIndexBuilder(storageStructureIdAndFName.fName, DataType(INT64));
    auto numEntries = 10;
    hashIndexBuilder.bulkReserve(10);
    for (uint64_t i = 0; i < numEntries; i++) {
        ASSERT_TRUE(hashIndexBuilder.append(i, i << 1));
    }
    for (uint64_t i = 0; i < numEntries; i++) {
        ASSERT_FALSE(hashIndexBuilder.append(i, i << 1));
    }
    hashIndexBuilder.flush();
}

TEST_F(HashIndexTest, HashIndexStringKeyInsertExists) {
    auto bufferManager = make_unique<BufferManager>();
    InMemHashIndexBuilder hashIndexBuilder(storageStructureIdAndFName.fName, DataType(STRING));
    char const* strKeys[] = {"abc", "def", "ghi", "jkl", "mno"};
    hashIndexBuilder.bulkReserve(5);
    for (auto i = 0u; i < 5; i++) {
        ASSERT_TRUE(hashIndexBuilder.append(strKeys[i], i));
    }
    for (auto i = 0u; i < 5; i++) {
        ASSERT_FALSE(hashIndexBuilder.append(strKeys[i], i));
    }
    hashIndexBuilder.flush();
}

static void parallel_insert(InMemHashIndexBuilder* index, int64_t startId, uint64_t num) {
    for (auto i = 0u; i < num; i++) {
        index->append(startId + i, (startId + i) * 2);
    }
}

TEST_F(HashIndexTest, ParallelHashIndexInsertions) {
    auto bufferManager = make_unique<BufferManager>();
    auto hashIndexBuilder =
        make_unique<InMemHashIndexBuilder>(storageStructureIdAndFName.fName, DataType(INT64));
    auto numKeysToInsert = 5000;
    hashIndexBuilder->bulkReserve(numKeysToInsert);
    auto numThreads = 10u;
    auto numKeysPerThread = numKeysToInsert / numThreads;
    thread threads[numThreads];
    auto startId = 0;
    for (auto i = 0u; i < numThreads; i++) {
        threads[i] = thread(parallel_insert, hashIndexBuilder.get(), startId, numKeysPerThread);
        startId += numKeysPerThread;
    }
    for (auto i = 0u; i < numThreads; i++) {
        threads[i].join();
    }
    hashIndexBuilder->flush();

    auto hashIndex =
        make_unique<HashIndex>(storageStructureIdAndFName, DataType(INT64), *bufferManager, true);
    node_offset_t result;
    for (auto i = 0u; i < numKeysToInsert; i++) {
        ASSERT_TRUE(hashIndex->lookup(i, result));
        ASSERT_EQ(result, i * 2);
    }
}
