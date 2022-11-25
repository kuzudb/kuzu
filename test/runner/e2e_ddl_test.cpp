#include "processor/mapper/plan_mapper.h"
#include "test_helper/test_helper.h"

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
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/primary-key-tests/int-pk-tests/");
    }

    void testPrimaryKey(string pkColName) {
        conn->query("CREATE NODE TABLE Person(firstIntCol INT64, name STRING, secondIntCol INT64, "
                    "PRIMARY KEY (" +
                    pkColName + "))");
        conn->query("CREATE REL TABLE Knows(From Person TO Person)");
        conn->query(
            "COPY Person FROM \"" +
            TestHelper::appendKuzuRootPath("dataset/primary-key-tests/int-pk-tests/vPerson.csv\""));
        conn->query(
            "COPY Knows FROM \"" +
            TestHelper::appendKuzuRootPath("dataset/primary-key-tests/int-pk-tests/eKnows.csv\""));
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
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/primary-key-tests/string-pk-tests/");
    }

    void testPrimaryKey(string pkColName) {
        conn->query("CREATE NODE TABLE Person(firstStrCol STRING, age INT64, secondStrCol STRING, "
                    "PRIMARY KEY (" +
                    pkColName + "))");
        conn->query("CREATE REL TABLE Knows(From Person TO Person)");
        conn->query(
            "COPY Person FROM \"" + TestHelper::appendKuzuRootPath(
                                        "dataset/primary-key-tests/string-pk-tests/vPerson.csv\""));
        conn->query(
            "COPY Knows FROM \"" + TestHelper::appendKuzuRootPath(
                                       "dataset/primary-key-tests/string-pk-tests/eKnows.csv\""));
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
        catalog = getCatalog(*database);
        profiler = make_unique<Profiler>();
        bufferManager =
            make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        executionContext = make_unique<ExecutionContext>(
            1 /* numThreads */, profiler.get(), memoryManager.get(), bufferManager.get());
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = getCatalog(*database);
    }

    string getInputCSVDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }

    // Since DDL statements are in an auto-commit transaction, we can't use the query interface to
    // test the recovery algorithm and parallel read.
    void createNodeTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            "CREATE NODE TABLE EXAM_PAPER(STUDENT_ID INT64, MARK DOUBLE, PRIMARY KEY(STUDENT_ID))");
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
            ASSERT_EQ(getStorageManager(*database)
                          ->getNodesStore()
                          .getNodesStatisticsAndDeletedIDs()
                          .getNumNodeStatisticsAndDeleteIDsPerTable(),
                3);
            initWithoutLoadingGraph();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
            ASSERT_EQ(getStorageManager(*database)
                          ->getNodesStore()
                          .getNodesStatisticsAndDeletedIDs()
                          .getNumNodeStatisticsAndDeleteIDsPerTable(),
                4);
        } else {
            conn->commit();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
            ASSERT_EQ(getStorageManager(*database)
                          ->getNodesStore()
                          .getNodesStatisticsAndDeletedIDs()
                          .getNumNodeStatisticsAndDeleteIDsPerTable(),
                4);
        }
    }

    void createRelTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            "CREATE REL TABLE likes(FROM person TO person | organisation, RATING INT64, MANY_ONE)");
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(getStorageManager(*database)->getRelsStore().getNumRelTables(), 6);
            initWithoutLoadingGraph();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(getStorageManager(*database)->getRelsStore().getNumRelTables(), 7);
        } else {
            conn->commit();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(getStorageManager(*database)->getRelsStore().getNumRelTables(), 7);
        }
    }

    void validateBelongsRelTable() {
        // Valid relations in belongs table: person->organisation, organisation->country.
        auto result = conn->query("MATCH (:person)-[:belongs]->(:organisation) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(TestHelper::convertResultToString(*result), vector<string>{"2"});
        result = conn->query("MATCH (:person)-[:belongs]->(:country) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(), "Binder exception: Node table person doesn't connect "
                                             "to country through rel table belongs.");
        result = conn->query("MATCH (:organisation)-[:belongs]->(:country) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(TestHelper::convertResultToString(*result), vector<string>{"1"});
        result = conn->query("MATCH (:organisation)-[:belongs]->(:person) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Binder exception: Node table organisation doesn't connect "
            "to person through rel table belongs.");
        result = conn->query("MATCH (:country)-[:belongs]->(:person) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(), "Binder exception: Node table country doesn't connect "
                                             "to person through rel table belongs.");
        result = conn->query("MATCH (:country)-[:belongs]->(:organisation) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(), "Binder exception: Node table country doesn't connect "
                                             "to organisation through rel table belongs.");
    }

    void createRelMixedRelationCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query("CREATE NODE TABLE country(id INT64, PRIMARY KEY(id));");
        conn->query("CREATE (c:country{id: 0});");
        executeQueryWithoutCommit(
            "CREATE REL TABLE belongs(FROM person TO organisation, FROM organisation TO country);");
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("belongs"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            initWithoutLoadingGraph();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("belongs"));
        } else {
            conn->commit();
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("belongs"));
        }
        auto relTableSchema = catalog->getReadOnlyVersion()->getRelTableSchema(
            catalog->getReadOnlyVersion()->getRelTableIDFromName("belongs"));
        validateRelColumnAndListFilesExistence(
            relTableSchema, DBFileType::ORIGINAL, true /* existence */);
        executeQueryWithoutCommit("COPY belongs FROM \"" +
                                  TestHelper::appendKuzuRootPath("dataset/tinysnb/eBelongs.csv\""));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            initWithoutLoadingGraph();
            validateBelongsRelTable();
        } else {
            conn->commit();
            validateBelongsRelTable();
        }
    }

    void dropNodeTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query("CREATE NODE TABLE university(address STRING, PRIMARY KEY(address));");
        auto nodeTableSchema =
            make_unique<NodeTableSchema>(*catalog->getReadOnlyVersion()->getNodeTableSchema(
                catalog->getReadOnlyVersion()->getNodeTableIDFromName("university")));
        executeQueryWithoutCommit("DROP TABLE university");
        validateNodeColumnFilesExistence(nodeTableSchema.get(), DBFileType::ORIGINAL, true);
        ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("university"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateNodeColumnFilesExistence(nodeTableSchema.get(), DBFileType::ORIGINAL, true);
            ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("university"));
            initWithoutLoadingGraph();
            validateNodeColumnFilesExistence(nodeTableSchema.get(), DBFileType::ORIGINAL, false);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("university"));
        } else {
            conn->commit();
            validateNodeColumnFilesExistence(nodeTableSchema.get(), DBFileType::ORIGINAL, false);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("university"));
        }
    }

    void dropRelTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        auto relTableSchema =
            make_unique<RelTableSchema>(*catalog->getReadOnlyVersion()->getRelTableSchema(
                catalog->getReadOnlyVersion()->getRelTableIDFromName("knows")));
        executeQueryWithoutCommit("DROP TABLE knows");
        validateRelColumnAndListFilesExistence(relTableSchema.get(), DBFileType::ORIGINAL, true);
        ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("knows"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
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

    void executeQueryWithoutCommit(string query) {
        auto preparedStatement = conn->prepare(query);
        conn->beginWriteTransaction();
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlan.get());
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
    }

    Catalog* catalog;
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
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
    result = conn->query("DROP TABLE movies");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("movies"));
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

TEST_F(TinySnbDDLTest, CreateMixedRelationTableNormalExecution) {
    createRelMixedRelationCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateMixedRelationTableRecovery) {
    createRelMixedRelationCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}
