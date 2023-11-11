#include <set>

#include "binder/bound_statement_result.h"
#include "graph_test/graph_test.h"
#include "processor/plan_mapper.h"
#include "processor/processor.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;
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
        executionContext = std::make_unique<ExecutionContext>(1 /* numThreads */, profiler.get(),
            getMemoryManager(*database), getBufferManager(*database), getClientContext(*conn));
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
        ASSERT_EQ(std::make_unique<Connection>(database.get())
                      ->query("MATCH (p:person) return *")
                      ->getNumTuples(),
            0);
        ASSERT_EQ(getStorageManager(*database)->getNodesStatisticsAndDeletedIDs()->getMaxNodeOffset(
                      &transaction::DUMMY_READ_TRANSACTION, tableID),
            UINT64_MAX);
    }

    void validateDatabaseStateAfterCheckPointCopyNode(table_id_t tableID) {
        auto nodeTableSchema =
            (NodeTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(tableID);
        validateTinysnbPersonAgeProperty();
        ASSERT_EQ(getStorageManager(*database)->getNodesStatisticsAndDeletedIDs()->getMaxNodeOffset(
                      &transaction::DUMMY_READ_TRANSACTION, tableID),
            7);
    }

    void copyNodeCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCMD);
        auto preparedStatement = conn->prepare(copyPersonTableCMD);
        conn->query("BEGIN TRANSACTION");
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan =
            mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[0].get(),
                preparedStatement->statementResult->getColumns());
        executionContext->clientContext->resetActiveQuery();
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
        auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
        validateDatabaseStateBeforeCheckPointCopyNode(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            validateDatabaseStateBeforeCheckPointCopyNode(tableID);
            physicalPlan.reset();
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyNode(tableID);
        } else {
            conn->query("COMMIT");
            validateDatabaseStateAfterCheckPointCopyNode(tableID);
        }
    }

    void validateTinysnbKnowsDateProperty() {
        std::multiset<date_t, std::greater<>> expectedResult = {
            Date::fromCString("1905-12-12", strlen("1905-12-12")),
            Date::fromCString("1905-12-12", strlen("1905-12-12")),
            Date::fromCString("1950-05-14", strlen("1950-05-14")),
            Date::fromCString("1950-05-14", strlen("1950-05-14")),
            Date::fromCString("1950-05-14", strlen("1950-05-14")),
            Date::fromCString("1950-05-14", strlen("1950-05-14")),
            Date::fromCString("2000-01-01", strlen("2000-01-01")),
            Date::fromCString("2000-01-01", strlen("2000-01-01")),
            Date::fromCString("2021-06-30", strlen("2021-06-30")),
            Date::fromCString("2021-06-30", strlen("2021-06-30")),
            Date::fromCString("2021-06-30", strlen("2021-06-30")),
            Date::fromCString("2021-06-30", strlen("2021-06-30")),
            Date::fromCString("2021-06-30", strlen("2021-06-30")),
            Date::fromCString("2021-06-30", strlen("2021-06-30"))};
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
        auto dummyWriteTrx = transaction::Transaction::getDummyWriteTrx();
        ASSERT_EQ(getStorageManager(*database)->getRelsStatistics()->getNextRelOffset(
                      dummyWriteTrx.get(), tableID),
            14);
    }

    void validateDatabaseStateAfterCheckPointCopyRel(table_id_t knowsTableID) {
        auto relTableSchema =
            (RelTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(knowsTableID);
        validateTinysnbKnowsDateProperty();
        auto relsStatistics = getStorageManager(*database)->getRelsStatistics();
        auto dummyWriteTrx = transaction::Transaction::getDummyWriteTrx();
        ASSERT_EQ(relsStatistics->getNextRelOffset(dummyWriteTrx.get(), knowsTableID), 14);
        ASSERT_EQ(relsStatistics->getReadOnlyVersion()->tableStatisticPerTable.size(), 1);
        auto knowsRelStatistics = (RelTableStats*)relsStatistics->getReadOnlyVersion()
                                      ->tableStatisticPerTable.at(knowsTableID)
                                      .get();
        ASSERT_EQ(knowsRelStatistics->getNumTuples(), 14);
    }

    void copyRelCSVCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query(createPersonTableCMD);
        conn->query(copyPersonTableCMD);
        conn->query(createKnowsTableCMD);
        auto preparedStatement = conn->prepare(copyKnowsTableCMD);
        conn->query("BEGIN TRANSACTION");
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan =
            mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[0].get(),
                preparedStatement->statementResult->getColumns());
        executionContext->clientContext->resetActiveQuery();
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
        auto tableID = catalog->getReadOnlyVersion()->getTableID("knows");
        validateDatabaseStateBeforeCheckPointCopyRel(tableID);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            validateDatabaseStateBeforeCheckPointCopyRel(tableID);
            physicalPlan.reset();
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCheckPointCopyRel(tableID);
        } else {
            conn->query("COMMIT");
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
        "create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, "
        "validInterval INTERVAL, comments STRING[], summary STRUCT(locations STRING[], transfer "
        "STRUCT(day DATE, amount INT64[])), notes UNION(firstmet DATE, type INT16, comment "
        "STRING), MANY_MANY);";
    std::string copyKnowsTableCMD =
        "COPY knows FROM \"" + TestHelper::appendKuzuRootPath("dataset/tinysnb/eKnows.csv\"");
    std::unique_ptr<Profiler> profiler;
    std::unique_ptr<ExecutionContext> executionContext;
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

} // namespace testing
} // namespace kuzu
