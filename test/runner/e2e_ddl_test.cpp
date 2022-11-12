#include "test/test_utility/include/test_helper.h"

#include "src/processor/mapper/include/plan_mapper.h"

using namespace kuzu::testing;

namespace kuzu {
namespace transaction {

class PrimaryKeyTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        databaseConfig->inMemoryMode = true;
        createDBAndConn();
    }
};

class IntPrimaryKeyTest : public PrimaryKeyTest {
public:
    string getInputCSVDir() override { return "dataset/primary-key-tests/int-pk-tests/"; }

    void testPrimaryKey(string pkColName) {
        conn->query("CREATE NODE TABLE Person(firstIntCol INT64, name STRING, secondIntCol INT64, "
                    "PRIMARY KEY (" +
                    pkColName + "))");
        conn->query("CREATE REL TABLE Knows(From Person TO Person)");
        conn->query("COPY Person FROM \"dataset/primary-key-tests/int-pk-tests/vPerson.csv\"");
        conn->query("COPY Knows FROM \"dataset/primary-key-tests/int-pk-tests/eKnows.csv\"");
        auto tuple = conn->query("MATCH (a:Person)-[e:Knows]->(b:Person) WHERE a.firstIntCol = 0"
                                 "RETURN COUNT(*)")
                         ->getNext();
        // Edge is from 0->1, and when the primary key is firstIntCol, we expect to find 1 result.
        // If key is secondIntCol we expect to find 0 result.
        if (pkColName == "firstIntCol") {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 1);
        } else {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 0);
        }
        tuple = conn->query("MATCH (a:Person)-[e:Knows]->(b:Person) WHERE a.firstIntCol = 1 "
                            "RETURN COUNT(*)")
                    ->getNext();
        // Edge is from 0->1, and when the primary key is firstIntCol, we expect to find 0 result.
        // If key is secondIntCol we expect to find 1 result.
        if (pkColName == "firstIntCol") {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 0);
        } else {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 1);
        }
    }
};

class StringPrimaryKeyTest : public PrimaryKeyTest {
public:
    string getInputCSVDir() override { return "dataset/primary-key-tests/string-pk-tests/"; }

    void testPrimaryKey(string pkColName) {
        conn->query("CREATE NODE TABLE Person(firstStrCol STRING, age INT64, secondStrCol STRING, "
                    "PRIMARY KEY (" +
                    pkColName + "))");
        conn->query("CREATE REL TABLE Knows(From Person TO Person)");
        conn->query("COPY Person FROM \"dataset/primary-key-tests/string-pk-tests/vPerson.csv\"");
        conn->query("COPY Knows FROM \"dataset/primary-key-tests/string-pk-tests/eKnows.csv\"");
        auto tuple =
            conn->query("MATCH (a:Person)-[e:Knows]->(b:Person) WHERE a.firstStrCol = \"Alice\" "
                        "RETURN COUNT(*)")
                ->getNext();
        // Edge is from "Alice"->"Bob", and when the primary key is firstStrCol, we expect to find 1
        // result. If key is secondStrCol we expect to find 0 result.
        if (pkColName == "firstStrCol") {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 1);
        } else {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 0);
        }
        tuple = conn->query("MATCH (a:Person)-[e:Knows]->(b:Person) WHERE a.firstStrCol = \"Bob\" "
                            "RETURN COUNT(*)")
                    ->getNext();
        // Edge is from "Alice"->"Bob", and when the primary key is firstStrCol, we expect to find 0
        // result. If key is secondStrCol we expect to find 1 result.
        if (pkColName == "firstStrCol") {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 0);
        } else {
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 1);
        }
    }
};

class TinySnbDDLTest : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        catalog = conn->database->getCatalog();
        profiler = make_unique<Profiler>();
        executionContext = make_unique<ExecutionContext>(1 /* numThreads */, profiler.get(),
            database->getMemoryManager(), database->getBufferManager());
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = conn->database->getCatalog();
    }

    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    // Since DDL statements are in an auto-commit transaction, we can't use the query interface to
    // test the recovery algorithm and parallel read.
    void createNodeTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        executeDDLWithoutCommit(
            "CREATE NODE TABLE EXAM_PAPER(STUDENT_ID INT64, MARK DOUBLE, PRIMARY KEY(STUDENT_ID))");
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesStatisticsAndDeletedIDs()
                          .getNumNodeStatisticsAndDeleteIDsPerTable(),
                2);
            initWithoutLoadingGraph();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesStatisticsAndDeletedIDs()
                          .getNumNodeStatisticsAndDeleteIDsPerTable(),
                3);
        } else {
            conn->commit();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
            ASSERT_EQ(database->getStorageManager()
                          ->getNodesStore()
                          .getNodesStatisticsAndDeletedIDs()
                          .getNumNodeStatisticsAndDeleteIDsPerTable(),
                3);
        }
    }

    void createRelTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        executeDDLWithoutCommit(
            "CREATE REL TABLE likes(FROM person TO person | organisation, RATING INT64, MANY_ONE)");
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(database->getStorageManager()->getRelsStore().getNumRelTables(), 6);
            initWithoutLoadingGraph();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(database->getStorageManager()->getRelsStore().getNumRelTables(), 7);
        } else {
            conn->commit();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(database->getStorageManager()->getRelsStore().getNumRelTables(), 7);
        }
    }

    void dropNodeTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query("CREATE NODE TABLE university(address STRING, PRIMARY KEY(address));");
        auto nodeTableSchema =
            make_unique<NodeTableSchema>(*catalog->getReadOnlyVersion()->getNodeTableSchema(
                catalog->getReadOnlyVersion()->getNodeTableIDFromName("university")));
        executeDDLWithoutCommit("DROP TABLE university");
        validateNodeColumnAndListFilesExistence(nodeTableSchema.get(), DBFileType::ORIGINAL, true);
        ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("university"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            validateNodeColumnAndListFilesExistence(
                nodeTableSchema.get(), DBFileType::ORIGINAL, true);
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("university"));
            initWithoutLoadingGraph();
            validateNodeColumnAndListFilesExistence(
                nodeTableSchema.get(), DBFileType::ORIGINAL, false);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("university"));
        } else {
            conn->commit();
            validateNodeColumnAndListFilesExistence(
                nodeTableSchema.get(), DBFileType::ORIGINAL, false);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("university"));
        }
    }

    void dropRelTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        auto relTableSchema =
            make_unique<RelTableSchema>(*catalog->getReadOnlyVersion()->getRelTableSchema(
                catalog->getReadOnlyVersion()->getRelTableIDFromName("knows")));
        executeDDLWithoutCommit("DROP TABLE knows");
        validateRelColumnAndListFilesExistence(relTableSchema.get(), DBFileType::ORIGINAL, true);
        ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("knows"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->commitButSkipCheckpointingForTestingRecovery();
            validateRelColumnAndListFilesExistence(
                relTableSchema.get(), DBFileType::ORIGINAL, true);
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("knows"));
            initWithoutLoadingGraph();
            validateRelColumnAndListFilesExistence(
                relTableSchema.get(), DBFileType::ORIGINAL, false);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("knows"));
        } else {
            conn->commit();
            validateRelColumnAndListFilesExistence(
                relTableSchema.get(), DBFileType::ORIGINAL, false);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("knows"));
        }
    }

    void ddlStatementsInsideActiveTransactionErrorTest(string query) {
        conn->beginWriteTransaction();
        auto result = conn->query(query);
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "DDL and CopyCSV statements are automatically wrapped in a transaction and committed. "
            "As such, they cannot be part of an active transaction, please commit or rollback your "
            "previous transaction and issue a ddl query without opening a transaction.");
    }

    void executeDDLWithoutCommit(string query) {
        auto preparedStatement = conn->prepareNoLock(query);
        conn->beginTransactionNoLock(WRITE);
        auto mapper = PlanMapper(
            *database->storageManager, database->getMemoryManager(), database->catalog.get());
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlan.get());
        database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
    }

    Catalog* catalog;
    unique_ptr<ExecutionContext> executionContext;
    unique_ptr<Profiler> profiler;
};
} // namespace transaction
} // namespace kuzu

TEST_F(StringPrimaryKeyTest, PrimaryKeyFirstColumn) {
    testPrimaryKey("firstStrCol");
}

TEST_F(StringPrimaryKeyTest, PrimaryKeySecondColumn) {
    testPrimaryKey("secondStrCol");
}

TEST_F(IntPrimaryKeyTest, PrimaryKeyFirstColumn) {
    testPrimaryKey("firstIntCol");
}

TEST_F(IntPrimaryKeyTest, PrimaryKeySecondColumn) {
    testPrimaryKey("secondIntCol");
}

TEST_F(TinySnbDDLTest, MultipleCreateNodeTables) {
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING, REGISTER_TIME DATE, PRIMARY KEY(NAME))");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("UNIVERSITY"));
    result = conn->query(
        "CREATE NODE TABLE STUDENT(STUDENT_ID INT64, NAME STRING, PRIMARY KEY(STUDENT_ID))");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("UNIVERSITY"));
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("STUDENT"));
    result = conn->query("MATCH (S:STUDENT) return S.STUDENT_ID");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (U:UNIVERSITY) return U.NAME");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (C:COLLEGE) return C.NAME");
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(TinySnbDDLTest, CreateNodeAfterCreateNodeTable) {
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING, PRIMARY KEY(NAME))");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("UNIVERSITY"));
    result =
        conn->query("CREATE (university:UNIVERSITY {NAME: 'WATERLOO', WEBSITE: 'WATERLOO.CA'})");
    ASSERT_TRUE(result->isSuccess());
    result = conn->query("MATCH (a:UNIVERSITY) RETURN a;");
    auto groundTruth = vector<string>{"WATERLOO|WATERLOO.CA"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

TEST_F(TinySnbDDLTest, DDLStatementWithActiveTransactionError) {
    ddlStatementsInsideActiveTransactionErrorTest(
        "CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
        "STRING, REGISTER_TIME DATE, PRIMARY KEY (NAME))");
    ddlStatementsInsideActiveTransactionErrorTest("DROP TABLE knows");
}

TEST_F(TinySnbDDLTest, CreateNodeTableCommitNormalExecution) {
    createNodeTableCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateNodeTableCommitRecovery) {
    createNodeTableCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, MultipleCreateRelTables) {
    auto result = conn->query("CREATE REL TABLE likes (FROM person TO person, FROM person TO "
                              "organisation, date DATE, MANY_MANY)");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
    result =
        conn->query("CREATE REL TABLE pays (FROM organisation TO person , date DATE, MANY_MANY)");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("pays"));
    result = conn->query("MATCH (p:person)-[l:likes]->(p1:person) return l.date");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (o:organisation)-[l:pays]->(p1:person) return l.date");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("MATCH (o:organisation)-[l:employees]->(p1:person) return l.ID");
    ASSERT_FALSE(result->isSuccess());
    result = conn->query("CREATE (p:person {ID:100})-[:likes]->(p1:person {ID:101})");
    ASSERT_TRUE(result->isSuccess());
    result = conn->query("MATCH (p:person)-[l:likes]->(p1:person) return l");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 1);
}

TEST_F(TinySnbDDLTest, CreateRelTableCommitNormalExecution) {
    createRelTableCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateRelTableCommitRecovery) {
    createRelTableCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, MultipleDropTables) {
    auto result = conn->query("DROP TABLE studyAt");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("studyAt"));
    result = conn->query("DROP TABLE workAt");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("workAt"));
    result = conn->query("DROP TABLE mixed");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("mixed"));
    result = conn->query("DROP TABLE organisation");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("organisation"));
    result = conn->query(
        "create node table organisation (ID INT64, name STRING, orgCode INT64, mark DOUBLE, score "
        "INT64, history STRING, licenseValidInterval INTERVAL, rating DOUBLE, PRIMARY KEY (ID));");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("organisation"));
    result =
        conn->query("create rel table studyAt (FROM person TO organisation, year INT64, places "
                    "STRING[], MANY_ONE);");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("studyAt"));
    result = conn->query("match (o:organisation) return o.name");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 0);
    result = conn->query("match (:person)-[s:studyAt]->(:organisation) return s.year");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->getNumTuples(), 0);
}

TEST_F(TinySnbDDLTest, DropNodeTableCommitNormalExecution) {
    dropNodeTableCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, DropNodeTableCommitRecovery) {
    dropNodeTableCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DropRelTableCommitNormalExecution) {
    dropRelTableCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, DropRelTableCommitRecovery) {
    dropRelTableCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DDLOutputMsg) {
    auto result = conn->query("CREATE NODE TABLE university(ID INT64, PRIMARY KEY(ID));");
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        vector<string>{"NodeTable: university has been created."});
    result = conn->query("CREATE REL TABLE nearTo(FROM university TO university, MANY_MANY);");
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        vector<string>{"RelTable: nearTo has been created."});
    result = conn->query("DROP TABLE nearTo;");
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        vector<string>{"RelTable: nearTo has been dropped."});
    result = conn->query("DROP TABLE university;");
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        vector<string>{"NodeTable: university has been dropped."});
}
