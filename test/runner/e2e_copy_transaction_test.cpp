#include <set>

#include "graph_test/graph_test.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/processor.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::testing;

namespace kuzu {
namespace testing {

class TinySnbCopyCSVTransactionTest : public EmptyDBTest {

public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
        catalog = getCatalog(*database);
        profiler = std::make_unique<Profiler>();
        clientContext = std::make_unique<ClientContext>();
        executionContext = std::make_unique<ExecutionContext>(1 /* numThreads */, profiler.get(),
            getMemoryManager(*database), getBufferManager(*database), clientContext.get());
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = getCatalog(*database);
    }

    void validateTinysnbPersonAgeProperty() {
        std::multiset<int, std::greater<>> expectedResult = {20, 20, 25, 30, 35, 40, 45, 83};
        std::multiset<int, std::greater<>> actualResult;
        auto queryResult = conn->query("match (p:person) return p.age");
        while (queryResult->hasNext()) {
            auto tuple = queryResult->getNext();
            actualResult.insert(tuple->getValue(0)->getValue<int64_t>());
        }
        ASSERT_EQ(expectedResult, actualResult);
    }

    void validateDatabaseStateBeforeCheckPointCopyNode(table_id_t tableID) {
        auto nodeTableSchema =
            (NodeTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(tableID);
        validateNodeColumnFilesExistence(
            nodeTableSchema, DBFileType::ORIGINAL, true /* existence */);
        ASSERT_EQ(std::make_unique<Connection>(database.get())
                      ->query("MATCH (p:person) return *")
                      ->getNumTuples(),
            0);
        ASSERT_EQ(getStorageManager(*database)
                      ->getNodesStore()
                      .getNodesStatisticsAndDeletedIDs()
                      .getMaxNodeOffset(transaction::TransactionType::READ_ONLY, tableID),
            UINT64_MAX);
    }

    void validateDatabaseStateAfterCheckPointCopyNode(table_id_t tableID) {
        auto nodeTableSchema =
            (NodeTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(tableID);
        validateNodeColumnFilesExistence(
            nodeTableSchema, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbPersonAgeProperty();
        ASSERT_EQ(getStorageManager(*database)
                      ->getNodesStore()
                      .getNodesStatisticsAndDeletedIDs()
                      .getMaxNodeOffset(transaction::TransactionType::READ_ONLY, tableID),
            7);
    }

    void copyNodeCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCMD);
        auto preparedStatement = conn->prepare(copyPersonTableCMD);
        conn->beginWriteTransaction();
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(
            preparedStatement->logicalPlans[0].get(), preparedStatement->getExpressionsToCollect());
        clientContext->activeQuery = std::make_unique<ActiveQuery>();
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
        auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
        validateDatabaseStateBeforeCheckPointCopyNode(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateDatabaseStateBeforeCheckPointCopyNode(tableID);
            physicalPlan.reset();
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyNode(tableID);
        } else {
            conn->commit();
            validateDatabaseStateAfterCheckPointCopyNode(tableID);
        }
    }

    void validateTinysnbKnowsDateProperty() {
        std::multiset<date_t, std::greater<>> expectedResult = {
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
        std::multiset<date_t, std::greater<>> actualResult;
        auto queryResult = conn->query("match (:person)-[e:knows]->(:person) return e.date");
        while (queryResult->hasNext()) {
            auto tuple = queryResult->getNext();
            actualResult.insert(tuple->getValue(0)->getValue<date_t>());
        }
        ASSERT_EQ(expectedResult, actualResult);
    }

    void validateDatabaseStateBeforeCheckPointCopyRel(table_id_t tableID) {
        auto relTableSchema =
            (RelTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(tableID);
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::ORIGINAL, true /* existence */);
        auto dummyWriteTrx = transaction::Transaction::getDummyWriteTrx();
        ASSERT_EQ(getStorageManager(*database)->getRelsStore().getRelsStatistics().getNextRelOffset(
                      dummyWriteTrx.get(), tableID),
            14);
    }

    void validateDatabaseStateAfterCheckPointCopyRel(table_id_t knowsTableID) {
        auto relTableSchema =
            (RelTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(knowsTableID);
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::ORIGINAL, true /* existence */);
        validateTinysnbKnowsDateProperty();
        auto& relsStatistics = getStorageManager(*database)->getRelsStore().getRelsStatistics();
        auto dummyWriteTrx = transaction::Transaction::getDummyWriteTrx();
        ASSERT_EQ(relsStatistics.getNextRelOffset(dummyWriteTrx.get(), knowsTableID), 14);
        ASSERT_EQ(relsStatistics.getReadOnlyVersion()->tableStatisticPerTable.size(), 1);
        auto knowsRelStatistics = (RelStatistics*)relsStatistics.getReadOnlyVersion()
                                      ->tableStatisticPerTable.at(knowsTableID)
                                      .get();
        ASSERT_EQ(knowsRelStatistics->getNumTuples(), 14);
    }

    void copyRelCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCMD);
        conn->query(copyPersonTableCMD);
        conn->query(createKnowsTableCMD);
        auto preparedStatement = conn->prepare(copyKnowsTableCMD);
        conn->beginWriteTransaction();
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(
            preparedStatement->logicalPlans[0].get(), preparedStatement->getExpressionsToCollect());
        clientContext->activeQuery = std::make_unique<ActiveQuery>();
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
        auto tableID = catalog->getReadOnlyVersion()->getTableID("knows");
        validateDatabaseStateBeforeCheckPointCopyRel(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateDatabaseStateBeforeCheckPointCopyRel(tableID);
            physicalPlan.reset();
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyRel(tableID);
        } else {
            conn->commit();
            validateDatabaseStateAfterCheckPointCopyRel(tableID);
        }
    }

    Catalog* catalog = nullptr;
    std::string createPersonTableCMD =
        "CREATE NODE TABLE person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, "
        "isWorker BOOLEAN, "
        "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
        "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], grades "
        "INT64[4], height float, "
        "PRIMARY KEY (ID))";
    std::string copyPersonTableCMD =
        "COPY person FROM \"" +
        TestHelper::appendKuzuRootPath("dataset/tinysnb/vPerson.csv\" (HEADER=true)");
    std::string createKnowsTableCMD =
        "CREATE REL TABLE knows (FROM person TO person, date DATE, meetTime TIMESTAMP, "
        "validInterval INTERVAL, comments STRING[], MANY_MANY)";
    std::string copyKnowsTableCMD =
        "COPY knows FROM \"" + TestHelper::appendKuzuRootPath("dataset/tinysnb/eKnows.csv\"");
    std::unique_ptr<Profiler> profiler;
    std::unique_ptr<ExecutionContext> executionContext;
    std::unique_ptr<ClientContext> clientContext;
};

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCommitNormalExecution) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeCommitRecovery) {
    copyNodeCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCommitNormalExecution) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyRelCommitRecovery) {
    copyRelCSVCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbCopyCSVTransactionTest, CopyNodeOutputMsg) {
    conn->query(createPersonTableCMD);
    conn->query(createKnowsTableCMD);
    auto result = conn->query(copyPersonTableCMD);
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        std::vector<std::string>{"8 number of tuples has been copied to table: person."});
    result = conn->query(copyKnowsTableCMD);
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        std::vector<std::string>{"14 number of tuples has been copied to table: knows."});
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

} // namespace testing
} // namespace kuzu
