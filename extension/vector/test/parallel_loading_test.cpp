#include "api_test/api_test.h"

#include "common/string_format.h"
#include "test_helper/test_helper.h"

namespace kuzu {
namespace testing {

class VectorParallelLoadingTest : public ApiTest {
protected:
    void loadVectorExtension() {
#ifndef __STATIC_LINK_EXTENSION_TEST__
        const auto extensionPath = TestHelper::appendKuzuRootPath(
            "extension/vector/build/libvector.kuzu_extension");
        ASSERT_TRUE(conn->query(common::stringFormat("LOAD EXTENSION '{}'", extensionPath))
                        ->isSuccess());
#endif
    }

    void SetUp() override {
        ApiTest::SetUp();
        loadVectorExtension();
    }
};

static std::string getEmbeddingsCSVPath() {
    return TestHelper::appendKuzuRootPath("dataset/embeddings/embeddings-8-1k.csv");
}

// The original upstream test validated that reopening the database (which triggers WAL replay)
// still leaves the HNSW index in a usable state. We recreate the essence here to make sure the
// extension works across multiple RELOAD DB operations.
TEST_F(VectorParallelLoadingTest, ReloadDatabaseKeepsVectorIndexUsable) {
    ASSERT_TRUE(conn->query(
                    "CREATE NODE TABLE embeddings (id INT64, vec FLOAT[8], PRIMARY KEY (id));")
                    ->isSuccess());

    ASSERT_TRUE(conn->query(common::stringFormat(
                        "COPY embeddings FROM '{}' (DELIM=',');", getEmbeddingsCSVPath()))
                    ->isSuccess());

    ASSERT_TRUE(conn->query(
                    "CALL CREATE_VECTOR_INDEX('embeddings', 'emb_idx', 'vec', metric := 'l2');")
                    ->isSuccess());

    // Initial query to force the index to be used once before reload.
    ASSERT_TRUE(conn
                    ->query("CALL QUERY_VECTOR_INDEX('embeddings', 'emb_idx', "
                            "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8], 3) RETURN node.id ORDER BY distance;" )
                    ->isSuccess());

    // First reload should succeed and WAL replay will synchronously load HNSW indexes.
    // Close and reopen database to trigger recovery
    conn.reset();
    database.reset();
    createDBAndConn();
    loadVectorExtension();

    // Run another query to ensure index remains available after recovery.
    ASSERT_TRUE(conn
                    ->query("CALL QUERY_VECTOR_INDEX('embeddings', 'emb_idx', "
                            "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8], 3) RETURN node.id ORDER BY distance;" )
                    ->isSuccess());

    // Repeat the cycle to mimic the failure scenario (second reload in e2e tests).
    conn.reset();
    database.reset();
    createDBAndConn();
    loadVectorExtension();

    ASSERT_TRUE(conn
                    ->query("CALL QUERY_VECTOR_INDEX('embeddings', 'emb_idx', "
                            "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8], 3) RETURN node.id ORDER BY distance;" )
                    ->isSuccess());
}

} // namespace testing
} // namespace kuzu
