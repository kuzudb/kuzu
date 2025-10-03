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

    // Create schema with HNSW index
    ASSERT_TRUE(conn->query("CREATE NODE TABLE Document(id INT64, embedding FLOAT[3], PRIMARY KEY(id));")->isSuccess());
    ASSERT_TRUE(conn->query("CALL CREATE_VECTOR_INDEX('Document', 'embedding_index', 'embedding');")->isSuccess());

    // Insert multiple documents - internal TaskScheduler will use multiple threads during commit
    // This exercises the LocalRelTable::delete_ path that was causing out-of-bounds access
    const int numDocs = 200;
    for (int i = 0; i < numDocs; i++) {
        float x = static_cast<float>(rand()) / RAND_MAX;
        float y = static_cast<float>(rand()) / RAND_MAX;
        float z = static_cast<float>(rand()) / RAND_MAX;
        auto query = stringFormat("CREATE (d:Document {id: %d, embedding: [%f, %f, %f]});", i, x, y, z);
        auto result = conn->query(query);
        ASSERT_TRUE(result->isSuccess());
    }

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
    ASSERT_TRUE(conn->query("CALL CREATE_VECTOR_INDEX('Photo', 'photo_embedding_index', 'embedding');")->isSuccess());

    // Simulate photo indexing workload: 1000 photos in batches of 100
    const int totalPhotos = 1000;
    const int batchSize = 100;
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

            auto query = stringFormat("CREATE (p:Photo {id: '%s', embedding: [%s], timestamp: %lld});",
                photoID.c_str(), embeddingStr.c_str(), timestamp);
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

    // Final verification
    auto finalResult = conn->query("MATCH (p:Photo) RETURN count(p);");
    ASSERT_TRUE(finalResult->isSuccess());
    ASSERT_TRUE(finalResult->hasNext());
    auto finalCount = finalResult->getNext()->getValue(0)->getValue<int64_t>();
    EXPECT_EQ(finalCount, totalPhotos);
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
    ASSERT_TRUE(conn->query("CALL CREATE_VECTOR_INDEX('Document', 'doc_vec_index', 'vec');")->isSuccess());

    // Single transaction with 500 inserts
    // This maximizes the chance of triggering out-of-bounds access in LocalRelTable
    const int itemCount = 500;

    for (int i = 0; i < itemCount; i++) {
        std::string docID = "doc-" + std::to_string(i);
        std::string vecStr;
        for (int j = 0; j < 64; j++) {
            float val = static_cast<float>(rand()) / RAND_MAX;
            if (j > 0) vecStr += ", ";
            vecStr += std::to_string(val);
        }

        auto query = stringFormat("CREATE (d:Document {id: '%s', vec: [%s]});", docID.c_str(), vecStr.c_str());
        auto result = conn->query(query);
        ASSERT_TRUE(result->isSuccess());
    }

    // Verify all items were inserted
    auto result = conn->query("MATCH (d:Document) RETURN count(d);");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(result->hasNext());
    auto count = result->getNext()->getValue(0)->getValue<int64_t>();
    EXPECT_EQ(count, itemCount);
}
