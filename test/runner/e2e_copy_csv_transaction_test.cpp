#include <set>

#include "processor/mapper/plan_mapper.h"
#include "test_helper/test_helper.h"

using namespace kuzu::testing;

namespace kuzu {
namespace transaction {

class TinySnbCopyCSVTransactionTest : public EmptyDBTest {

public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
        catalog = getCatalog(*database);
        profiler = make_unique<Profiler>();
        executionContext = make_unique<ExecutionContext>(1 /* numThreads */, profiler.get(),
            getMemoryManager(*database), getBufferManager(*database));
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = getCatalog(*database);
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
        validateNodeColumnFilesExistence(
            nodeTableSchema, DBFileType::WAL_VERSION, true /* existence */);
        validateNodeColumnFilesExistence(
            nodeTableSchema, DBFileType::ORIGINAL, true /* existence */);
        ASSERT_EQ(make_unique<Connection>(database.get())
                      ->query("MATCH (p:person) return *")
                      ->getNumTuples(),
            0);
        ASSERT_EQ(getStorageManager(*database)
                      ->getNodesStore()
                      .getNodesStatisticsAndDeletedIDs()
                      .getMaxNodeOffset(TransactionType::READ_ONLY, tableID),
            UINT64_MAX);
    }

    void validateDatabaseStateAfterCheckPointCopyNodeCSV(table_id_t tableID) {
        auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
        // After checkPointing, we should only have one version of node column and list
        // files(original version). The updates to maxNodeOffset should be visible to read-only
        // transaction;
        validateNodeColumnFilesExistence(
            nodeTableSchema, DBFileType::WAL_VERSION, false /* existence */);
        validateNodeColumnFilesExistence(
            nodeTableSchema, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbPersonAgeProperty();
        ASSERT_EQ(getStorageManager(*database)
                      ->getNodesStore()
                      .getNodesStatisticsAndDeletedIDs()
                      .getMaxNodeOffset(TransactionType::READ_ONLY, tableID),
            7);
    }

    void copyNodeCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCMD);
        auto preparedStatement = conn->prepare(copyPersonTableCMD);
        conn->beginWriteTransaction();
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlan.get());
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
        auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("person");
        validateDatabaseStateBeforeCheckPointCopyNodeCSV(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
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
        auto dummyWriteTrx = Transaction::getDummyWriteTrx();
        ASSERT_EQ(getStorageManager(*database)->getRelsStore().getRelsStatistics().getNextRelID(
                      dummyWriteTrx.get()),
            14);
    }

    void validateDatabaseStateAfterCheckPointCopyRelCSV(table_id_t knowsTableID) {
        auto relTableSchema = catalog->getReadOnlyVersion()->getRelTableSchema(knowsTableID);
        // After checkPointing, we should only have one version of rel column and list
        // files(original version).
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::WAL_VERSION, false /* existence */);
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbKnowsDateProperty();
        auto& relsStatistics = getStorageManager(*database)->getRelsStore().getRelsStatistics();
        auto dummyWriteTrx = Transaction::getDummyWriteTrx();
        ASSERT_EQ(relsStatistics.getNextRelID(dummyWriteTrx.get()), 14);
        ASSERT_EQ(relsStatistics.getReadOnlyVersion()->tableStatisticPerTable.size(), 1);
        auto knowsRelStatistics = (RelStatistics*)relsStatistics.getReadOnlyVersion()
                                      ->tableStatisticPerTable.at(knowsTableID)
                                      .get();
        ASSERT_EQ(knowsRelStatistics->getNumTuples(), 14);
        ASSERT_EQ(knowsRelStatistics->getNumRelsForDirectionBoundTable(RelDirection::FWD, 0), 14);
        ASSERT_EQ(knowsRelStatistics->getNumRelsForDirectionBoundTable(RelDirection::BWD, 0), 14);
    }

    void copyRelCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCMD);
        conn->query(copyPersonTableCMD);
        conn->query(createKnowsTableCMD);
        auto preparedStatement = conn->prepare(copyKnowsTableCMD);
        conn->beginWriteTransaction();
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlan.get());
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
        auto tableID = catalog->getReadOnlyVersion()->getRelTableIDFromName("knows");
        validateDatabaseStateBeforeCheckPointCopyRelCSV(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateDatabaseStateBeforeCheckPointCopyRelCSV(tableID);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyRelCSV(tableID);
        } else {
            conn->commit();
            validateDatabaseStateAfterCheckPointCopyRelCSV(tableID);
        }
    }

    Catalog* catalog = nullptr;
    string createPersonTableCMD =
        "CREATE NODE TABLE person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, "
        "isWorker BOOLEAN, "
        "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
        "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], "
        "PRIMARY KEY (ID))";
    string copyPersonTableCMD =
        "COPY person FROM \"" +
        TestHelper::appendKuzuRootPath("dataset/tinysnb/vPerson.csv\" (HEADER=true)");
    string createKnowsTableCMD =
        "CREATE REL TABLE knows (FROM person TO person, date DATE, meetTime TIMESTAMP, "
        "validInterval INTERVAL, comments STRING[], MANY_MANY)";
    string copyKnowsTableCMD =
        "COPY knows FROM \"" + TestHelper::appendKuzuRootPath("dataset/tinysnb/eKnows.csv\"");
    unique_ptr<Profiler> profiler;
    unique_ptr<ExecutionContext> executionContext;
};
} // namespace transaction
} // namespace kuzu

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCSVCommitNormalExecution) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCSVCommitRecovery) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCSVCommitNormalExecution) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCSVCommitRecovery) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCSVOutputMsg) {
    conn->query(createPersonTableCMD);
    conn->query(createKnowsTableCMD);
    auto result = conn->query(copyPersonTableCMD);
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        vector<string>{"8 number of nodes has been copied to nodeTable: person."});
    result = conn->query(copyKnowsTableCMD);
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        vector<string>{"14 number of rels has been copied to relTable: knows."});
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyCSVStatementWithActiveTransactionErrorTest) {
    auto re = conn->query(createPersonTableCMD);
    ASSERT_TRUE(re->isSuccess());
    conn->beginWriteTransaction();
    auto result = conn->query(copyPersonTableCMD);
    ASSERT_EQ(result->getErrorMessage(),
        "DDL and CopyCSV statements are automatically wrapped in a transaction and committed. "
        "As such, they cannot be part of an active transaction, please commit or rollback your "
        "previous transaction and issue a ddl query without opening a transaction.");
}
