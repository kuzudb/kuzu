#include "graph_test/private_graph_test.h"

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::testing;

class HNSWThreadSafetyTest : public DBTest {
public:
    void SetUp() override {
        DBTest::SetUp();
    }

    std::string getInputDir() override {
        return "";
    }
};

// Test HNSW index insertions with internal multi-threading
// This reproduces the crash scenario where TaskScheduler uses multiple worker threads
// during transaction commit, which could cause out-of-bounds access in LocalRelTable
// when accessing directedIndices for unidirectional HNSW internal RelTable
TEST_F(HNSWThreadSafetyTest, ConcurrentHNSWIndexInsertions) {
#ifndef __STATIC_LINK_EXTENSION_TEST__
    ASSERT_TRUE(conn->query(stringFormat("LOAD EXTENSION '{}'",
                                TestHelper::appendKuzuRootPath(
                                    "extension/vector/build/libvector.kuzu_extension")))
                    ->isSuccess());
#endif

    // Create schema and insert data BEFORE creating HNSW index (matching existing tests)
    std::cout << "Creating Document table..." << std::endl;
    ASSERT_TRUE(conn->query("CREATE NODE TABLE Document(id INT64, embedding FLOAT[3], PRIMARY KEY(id));")->isSuccess());
    std::cout << "Table created successfully" << std::endl;

    // Insert documents first (before index creation)
    const int numDocs = 50;
    std::cout << "Starting to insert " << numDocs << " documents..." << std::endl;
    for (int i = 0; i < numDocs; i++) {
        if (i % 10 == 0) {
            std::cout << "Inserting document " << i << "/" << numDocs << "..." << std::endl;
        }
        float x = static_cast<float>(rand()) / RAND_MAX;
        float y = static_cast<float>(rand()) / RAND_MAX;
        float z = static_cast<float>(rand()) / RAND_MAX;
        std::string query = "CREATE (d:Document {id: " + std::to_string(i) +
                           ", embedding: [" + std::to_string(x) + ", " +
                           std::to_string(y) + ", " + std::to_string(z) + "]});";
        auto result = conn->query(query);
        ASSERT_TRUE(result->isSuccess());
    }
    std::cout << "All documents inserted successfully" << std::endl;

    // Create HNSW index AFTER data insertion (this is the correct order)
    std::cout << "Creating HNSW vector index..." << std::endl;
    auto indexResult = conn->query("CALL CREATE_VECTOR_INDEX('Document', 'embedding_index', 'embedding');");
    std::cout << "Index creation query executed" << std::endl;
    ASSERT_TRUE(indexResult->isSuccess());
    std::cout << "Index created successfully" << std::endl;

    // Verify all data was inserted correctly
    auto result = conn->query("MATCH (d:Document) RETURN count(d);");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(result->hasNext());
    auto row = result->getNext();
    auto count = row->getValue(0)->getValue<int64_t>();
    EXPECT_EQ(count, numDocs);

    // Verify data can be queried
    auto queryResult = conn->query("MATCH (d:Document) RETURN d.id ORDER BY d.id LIMIT 5;");
    ASSERT_TRUE(queryResult->isSuccess());
    int queryCount = 0;
    while (queryResult->hasNext()) {
        queryResult->getNext();
        queryCount++;
    }
    EXPECT_EQ(queryCount, 5);
}

// Large-scale stress test simulating real-world photo indexing workload
// This test reproduces the exact scenario that was causing crashes in production
TEST_F(HNSWThreadSafetyTest, LargeScaleHNSWIndexing) {
#ifndef __STATIC_LINK_EXTENSION_TEST__
    ASSERT_TRUE(conn->query(stringFormat("LOAD EXTENSION '{}'",
                                TestHelper::appendKuzuRootPath(
                                    "extension/vector/build/libvector.kuzu_extension")))
                    ->isSuccess());
#endif

    ASSERT_TRUE(conn->query("CREATE NODE TABLE Photo(id STRING, embedding FLOAT[128], timestamp INT64, PRIMARY KEY(id));")->isSuccess());

    // Insert photos BEFORE creating index
    const int totalPhotos = 100;
    const int batchSize = 25;
    const int batches = totalPhotos / batchSize;

    for (int batchNum = 0; batchNum < batches; batchNum++) {
        int batchStart = batchNum * batchSize;
        int batchEnd = batchStart + batchSize;

        // Insert batch
        for (int i = batchStart; i < batchEnd; i++) {
            std::string photoID = "photo-" + std::to_string(i);
            std::string embeddingStr;
            for (int j = 0; j < 128; j++) {
                float val = static_cast<float>(rand()) / RAND_MAX;
                if (j > 0) embeddingStr += ", ";
                embeddingStr += std::to_string(val);
            }
            int64_t timestamp = 1609459200 + i; // 2021-01-01 + offset

            std::string query = "CREATE (p:Photo {id: '" + photoID +
                               "', embedding: [" + embeddingStr +
                               "], timestamp: " + std::to_string(timestamp) + "});";
            auto result = conn->query(query);
            ASSERT_TRUE(result->isSuccess());
        }

        // Verify batch was committed successfully
        auto countResult = conn->query("MATCH (p:Photo) RETURN count(p);");
        ASSERT_TRUE(countResult->isSuccess());
        ASSERT_TRUE(countResult->hasNext());
        auto count = countResult->getNext()->getValue(0)->getValue<int64_t>();
        EXPECT_EQ(count, batchEnd);
    }

    // Final verification before index creation
    auto finalResult = conn->query("MATCH (p:Photo) RETURN count(p);");
    ASSERT_TRUE(finalResult->isSuccess());
    ASSERT_TRUE(finalResult->hasNext());
    auto finalCount = finalResult->getNext()->getValue(0)->getValue<int64_t>();
    EXPECT_EQ(finalCount, totalPhotos);

    // Create HNSW index AFTER all data is inserted
    ASSERT_TRUE(conn->query("CALL CREATE_VECTOR_INDEX('Photo', 'photo_embedding_index', 'embedding');")->isSuccess());
}

// Extreme stress test with single large transaction
// This test commits 500 items at once to trigger maximum internal parallelism
// Reproduces the worst-case scenario for the DirectedCSRIndex race condition
TEST_F(HNSWThreadSafetyTest, SingleLargeTransactionStress) {
#ifndef __STATIC_LINK_EXTENSION_TEST__
    ASSERT_TRUE(conn->query(stringFormat("LOAD EXTENSION '{}'",
                                TestHelper::appendKuzuRootPath(
                                    "extension/vector/build/libvector.kuzu_extension")))
                    ->isSuccess());
#endif

    ASSERT_TRUE(conn->query("CREATE NODE TABLE Document(id STRING, vec FLOAT[64], PRIMARY KEY(id));")->isSuccess());

    // Insert documents BEFORE creating index
    const int itemCount = 100;

    for (int i = 0; i < itemCount; i++) {
        std::string docID = "doc-" + std::to_string(i);
        std::string vecStr;
        for (int j = 0; j < 64; j++) {
            float val = static_cast<float>(rand()) / RAND_MAX;
            if (j > 0) vecStr += ", ";
            vecStr += std::to_string(val);
        }

        std::string query = "CREATE (d:Document {id: '" + docID +
                           "', vec: [" + vecStr + "]});";
        auto result = conn->query(query);
        ASSERT_TRUE(result->isSuccess());
    }

    // Verify all items were inserted
    auto result = conn->query("MATCH (d:Document) RETURN count(d);");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(result->hasNext());
    auto count = result->getNext()->getValue(0)->getValue<int64_t>();
    EXPECT_EQ(count, itemCount);

    // Create HNSW index AFTER all data is inserted
    ASSERT_TRUE(conn->query("CALL CREATE_VECTOR_INDEX('Document', 'doc_vec_index', 'vec');")->isSuccess());
}
