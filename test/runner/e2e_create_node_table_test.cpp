#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

namespace graphflow {
namespace transaction {

class TinySnbCreateNodeTableTest : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    // Since DDL statements are in an auto-commit transaction, we can't use the query interface to
    // test the recovery algorithm and parallel read.
    void createNodeTableCommitAndRecoveryTest(bool testRecovery) {
        auto preparedStatement =
            conn->prepareNoLock("CREATE NODE TABLE EXAM_PAPER(STUDENT_ID INT64, MARK DOUBLE)");
        conn->beginTransactionNoLock(WRITE);
        auto profiler = make_unique<Profiler>();
        auto executionContext = make_unique<ExecutionContext>(1, profiler.get(), nullptr, nullptr);
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), executionContext.get());
        ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("EXAM_PAPER"), false);
        if (testRecovery) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("EXAM_PAPER"), false);
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesMetadata()
                          .getNumNodeMetadataPerLabel(),
                2);
            initWithoutLoadingGraph();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("EXAM_PAPER"), true);
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesMetadata()
                          .getNumNodeMetadataPerLabel(),
                3);
        } else {
            conn->commit();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("EXAM_PAPER"), true);
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesMetadata()
                          .getNumNodeMetadataPerLabel(),
                3);
        }
    }

    Catalog* catalog;
};
} // namespace transaction
} // namespace graphflow

TEST_F(TinySnbCreateNodeTableTest, MultipleCreateNodeTablesTest) {
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING, REGISTER_TIME DATE)");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("UNIVERSITY"), true);
    result = conn->query(
        "CREATE NODE TABLE STUDENT(STUDENT_ID INT64, NAME STRING, PRIMARY KEY(STUDENT_ID))");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("UNIVERSITY"), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("STUDENT"), true);
    result = conn->query("MATCH (S:STUDENT) return S.STUDENT_ID");
    ASSERT_EQ(result->isSuccess(), true);
    result = conn->query("MATCH (U:UNIVERSITY) return U.NAME");
    ASSERT_EQ(result->isSuccess(), true);
}

TEST_F(TinySnbCreateNodeTableTest, CreateNodeAfterCreateNodeTableTest) {
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING)");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeLabel("UNIVERSITY"), true);
    result = conn->query(
        "CREATE (university:UNIVERSITY {NAME: \"WATERLOO\", WEBSITE: \"WATERLOO.CA\"})");
    ASSERT_EQ(result->isSuccess(), true);
}

TEST_F(TinySnbCreateNodeTableTest, CreateNodeTableWithActiveTransactionErrorTest) {
    conn->beginWriteTransaction();
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING, REGISTER_TIME DATE)");
    ASSERT_EQ(result->getErrorMessage(),
        "DDL statements are automatically wrapped in a transaction and committed automatically. "
        "As such, they cannot be part of an active transaction, please commit or rollback your "
        "previous transaction and issue a ddl query without opening a transaction.");
}

TEST_F(TinySnbCreateNodeTableTest, CreateNodeTableCommitTest) {
    createNodeTableCommitAndRecoveryTest(false /* testRecovery */);
}

TEST_F(TinySnbCreateNodeTableTest, CreateNodeTableCommitRecoveryTest) {
    createNodeTableCommitAndRecoveryTest(true /* testRecovery */);
}
