#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

namespace graphflow {
namespace transaction {

class TinySnbDDLTest : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        catalog = conn->database->getCatalog();
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    // Since DDL statements are in an auto-commit transaction, we can't use the query interface to
    // test the recovery algorithm and parallel read.
    void createNodeTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        auto preparedStatement =
            conn->prepareNoLock("CREATE NODE TABLE EXAM_PAPER(STUDENT_ID INT64, MARK DOUBLE)");
        conn->beginTransactionNoLock(WRITE);
        auto profiler = make_unique<Profiler>();
        auto executionContext = make_unique<ExecutionContext>(1, profiler.get(), nullptr, nullptr);
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), executionContext.get());
        ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"), false);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"), false);
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesMetadata()
                          .getNumNodeMetadataPerTable(),
                2);
            initWithoutLoadingGraph();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"), true);
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesMetadata()
                          .getNumNodeMetadataPerTable(),
                3);
        } else {
            conn->commit();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"), true);
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesMetadata()
                          .getNumNodeMetadataPerTable(),
                3);
        }
    }

    void createRelTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        auto preparedStatement = conn->prepareNoLock(
            "CREATE REL likes(FROM person TO person | organisation, RATING INT64, MANY_ONE)");
        conn->beginTransactionNoLock(WRITE);
        auto profiler = make_unique<Profiler>();
        auto executionContext = make_unique<ExecutionContext>(1, profiler.get(), nullptr, nullptr);
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), executionContext.get());
        ASSERT_EQ(catalog->getReadOnlyVersion()->containRelTable("likes"), false);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containRelTable("likes"), false);
            ASSERT_EQ(database->getStorageManager()->getRelsStore().getNumRelTables(), 4);
            initWithoutLoadingGraph();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containRelTable("likes"), true);
            ASSERT_EQ(database->getStorageManager()->getRelsStore().getNumRelTables(), 5);
        } else {
            conn->commit();
            ASSERT_EQ(catalog->getReadOnlyVersion()->containRelTable("likes"), true);
            ASSERT_EQ(database->getStorageManager()->getRelsStore().getNumRelTables(), 5);
        }
    }

    Catalog* catalog;
};
} // namespace transaction
} // namespace graphflow

TEST_F(TinySnbDDLTest, MultipleCreateNodeTablesTest) {
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING, REGISTER_TIME DATE)");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("UNIVERSITY"), true);
    result = conn->query(
        "CREATE NODE TABLE STUDENT(STUDENT_ID INT64, NAME STRING, PRIMARY KEY(STUDENT_ID))");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("UNIVERSITY"), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("STUDENT"), true);
    result = conn->query("MATCH (S:STUDENT) return S.STUDENT_ID");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (U:UNIVERSITY) return U.NAME");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (C:COLLEGE) return C.NAME");
    ASSERT_EQ(result->isSuccess(), false);
}

TEST_F(TinySnbDDLTest, CreateNodeAfterCreateNodeTableTest) {
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING)");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containNodeTable("UNIVERSITY"), true);
    result = conn->query(
        "CREATE (university:UNIVERSITY {NAME: \"WATERLOO\", WEBSITE: \"WATERLOO.CA\"})");
    ASSERT_EQ(result->isSuccess(), true);
}

TEST_F(TinySnbDDLTest, DDLStatementWithActiveTransactionErrorTest) {
    conn->beginWriteTransaction();
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING, REGISTER_TIME DATE)");
    ASSERT_EQ(result->getErrorMessage(),
        "DDL and CopyCSV statements are automatically wrapped in a transaction and committed "
        "automatically. "
        "As such, they cannot be part of an active transaction, please commit or rollback your "
        "previous transaction and issue a ddl query without opening a transaction.");
}

TEST_F(TinySnbDDLTest, CreateNodeTableCommitTest) {
    createNodeTableCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateNodeTableCommitRecoveryTest) {
    createNodeTableCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, MultipleCreateRelTablesTest) {
    auto result = conn->query("CREATE REL likes (FROM person TO person, FROM person TO "
                              "organisation, date DATE, MANY_MANY)");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containRelTable("likes"), true);
    result = conn->query("CREATE REL pays (FROM organisation TO person , date DATE, MANY_MANY)");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containRelTable("likes"), true);
    ASSERT_EQ(catalog->getReadOnlyVersion()->containRelTable("pays"), true);
    result = conn->query("MATCH (p:person)-[l:likes]->(p1:person) return l.date");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (o:organisation)-[l:pays]->(p1:person) return l.date");
    ASSERT_EQ(result->isSuccess(), true);
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (o:organisation)-[l:employees]->(p1:person) return l.ID");
    ASSERT_EQ(result->isSuccess(), false);
}

TEST_F(TinySnbDDLTest, CreateRelTableCommitTest) {
    createRelTableCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateRelTableCommitRecoveryTest) {
    createRelTableCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}
