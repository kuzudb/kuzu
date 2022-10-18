#include <set>

#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

namespace graphflow {
namespace transaction {

class TinySnbCopyCSVTransactionTest : public EmptyDBTest {

public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    void validateTinysnbPersonAgeProperty() {
        multiset<int, greater<>> expectedResult = {20, 20, 25, 30, 35, 40, 45, 83};
        multiset<int, greater<>> actualResult;
        auto queryResult = conn->query("match (p:person) return p.age");
        while (queryResult->hasNext()) {
            auto tuple = queryResult->getNext();
            actualResult.insert(tuple->getResultValue(0)->getInt64Val());
        }
        ASSERT_EQ(expectedResult, actualResult);
    }

    void validateDatabaseStateBeforeCheckPointCopyNodeCSV(table_id_t tableID) {
        auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
        // Before checkPointing, we should have two versions of node column and list files. The
        // updates to maxNodeOffset should be invisible to read-only transactions.
        validateNodeColumnAndListFilesExistence(
            nodeTableSchema, DBFileType::WAL_VERSION, true /* existence */);
        validateNodeColumnAndListFilesExistence(
            nodeTableSchema, DBFileType::ORIGINAL, true /* existence */);
        ASSERT_EQ(make_unique<Connection>(database.get())
                      ->query("MATCH (p:person) return *")
                      ->getNumTuples(),
            0);
        ASSERT_EQ(database->storageManager->getNodesStore()
                      .getNodesStatisticsAndDeletedIDs()
                      .getMaxNodeOffset(TransactionType::READ_ONLY, tableID),
            UINT64_MAX);
    }

    void validateDatabaseStateAfterCheckPointCopyNodeCSV(table_id_t tableID) {
        auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
        // After checkPointing, we should only have one version of node column and list
        // files(original version). The updates to maxNodeOffset should be visible to read-only
        // transaction;
        validateNodeColumnAndListFilesExistence(
            nodeTableSchema, DBFileType::WAL_VERSION, false /* existence */);
        validateNodeColumnAndListFilesExistence(
            nodeTableSchema, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbPersonAgeProperty();
        ASSERT_EQ(database->getStorageManager()
                      ->getNodesStore()
                      .getNodesStatisticsAndDeletedIDs()
                      .getMaxNodeOffset(TransactionType::READ_ONLY, tableID),
            7);
    }

    void copyNodeCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCmd);
        auto preparedStatement = conn->prepareNoLock(copyPersonTableCmd);
        conn->beginTransactionNoLock(WRITE);
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), nullptr /* executionContext */);
        auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("person");
        validateDatabaseStateBeforeCheckPointCopyNodeCSV(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            validateDatabaseStateBeforeCheckPointCopyNodeCSV(tableID);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyNodeCSV(tableID);
        } else {
            conn->commit();
            validateDatabaseStateAfterCheckPointCopyNodeCSV(tableID);
        }
    }

    void validateTinysnbKnowsDateProperty() {
        multiset<date_t, greater<>> expectedResult = {
            Date::FromCString("1905-12-12", strlen("1905-12-12")),
            Date::FromCString("1905-12-12", strlen("1905-12-12")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("1950-05-14", strlen("1950-05-14")),
            Date::FromCString("2000-01-01", strlen("2000-01-01")),
            Date::FromCString("2000-01-01", strlen("2000-01-01")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30")),
            Date::FromCString("2021-06-30", strlen("2021-06-30"))};
        multiset<date_t, greater<>> actualResult;
        auto queryResult = conn->query("match (:person)-[e:knows]->(:person) return e.date");
        while (queryResult->hasNext()) {
            auto tuple = queryResult->getNext();
            actualResult.insert(tuple->getResultValue(0)->getDateVal());
        }
        ASSERT_EQ(expectedResult, actualResult);
    }

    void validateDatabaseStateBeforeCheckPointCopyRelCSV(table_id_t tableID) {
        auto relTableSchema = catalog->getReadOnlyVersion()->getRelTableSchema(tableID);
        // Before checkPointing, we should have two versions of rel column and list files.
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::WAL_VERSION, true /* existence */);
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::ORIGINAL, true /* existence */);
        ASSERT_EQ(database->storageManager->getRelsStore().getRelsStatistics().getNextRelID(), 0);
    }

    void validateDatabaseStateAfterCheckPointCopyRelCSV(table_id_t tableID) {
        auto relTableSchema = catalog->getReadOnlyVersion()->getRelTableSchema(tableID);
        // After checkPointing, we should only have one version of rel column and list
        // files(original version).
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::WAL_VERSION, false /* existence */);
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbKnowsDateProperty();
        auto& relsStatistics = database->storageManager->getRelsStore().getRelsStatistics();
        ASSERT_EQ(relsStatistics.getNextRelID(), 14);
        ASSERT_EQ(relsStatistics.getReadOnlyVersion()->size(), 1);
        auto knowsRelStatistics = (RelStatistics*)((*relsStatistics.getReadOnlyVersion())[0].get());
        ASSERT_EQ(knowsRelStatistics->getNumTuples(), 14);
        ASSERT_EQ(knowsRelStatistics->getNumRelsForDirectionBoundTable(RelDirection::FWD, 0), 14);
        ASSERT_EQ(knowsRelStatistics->getNumRelsForDirectionBoundTable(RelDirection::BWD, 0), 14);
    }

    void copyRelCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCmd);
        conn->query(copyPersonTableCmd);
        conn->query("create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, "
                    "validInterval INTERVAL, comments STRING[], MANY_MANY)");
        auto preparedStatement =
            conn->prepareNoLock("COPY knows FROM \"dataset/tinysnb/eKnows.csv\"");
        conn->beginTransactionNoLock(WRITE);
        auto profiler = make_unique<Profiler>();
        auto bufferManager = make_unique<BufferManager>();
        auto executionContext = make_unique<ExecutionContext>(
            1, profiler.get(), nullptr /* memoryManager */, bufferManager.get());
        database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), executionContext.get());
        auto tableID = catalog->getReadOnlyVersion()->getRelTableIDFromName("knows");
        validateDatabaseStateBeforeCheckPointCopyRelCSV(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            validateDatabaseStateBeforeCheckPointCopyRelCSV(tableID);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyRelCSV(tableID);
        } else {
            conn->commit();
            validateDatabaseStateAfterCheckPointCopyRelCSV(tableID);
        }
    }

    Catalog* catalog;
    string createPersonTableCmd =
        "create node table person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, "
        "isWorker BOOLEAN, "
        "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
        "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], "
        "PRIMARY KEY (ID))";
    string copyPersonTableCmd = "COPY person FROM \"dataset/tinysnb/vPerson.csv\" (HEADER=true)";
};
} // namespace transaction
} // namespace graphflow

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCSVCommitTest) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCSVCommitRecoveryTest) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCSVCommitTest) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCSVCommitRecoveryTest) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyCSVStatementWithActiveTransactionErrorTest) {
    auto re = conn->query(createPersonTableCmd);
    ASSERT_TRUE(re->isSuccess());
    conn->beginWriteTransaction();
    auto result = conn->query(copyPersonTableCmd);
    ASSERT_EQ(result->getErrorMessage(),
        "DDL and CopyCSV statements are automatically wrapped in a transaction and committed. "
        "As such, they cannot be part of an active transaction, please commit or rollback your "
        "previous transaction and issue a ddl query without opening a transaction.");
}
