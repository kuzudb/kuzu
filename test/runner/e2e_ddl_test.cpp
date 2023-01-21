#include "graph_test/graph_test.h"
#include "processor/mapper/plan_mapper.h"

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
    string getInputDir() override {
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
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 1);
        } else {
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 0);
        }
        tuple = conn->query("MATCH (a:Person)-[e:Knows]->(b:Person) WHERE a.firstIntCol = 1 "
                            "RETURN COUNT(*)")
                    ->getNext();
        // Edge is from 0->1, and when the primary key is firstIntCol, we expect to find 0 result.
        // If key is secondIntCol we expect to find 1 result.
        if (pkColName == "firstIntCol") {
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 0);
        } else {
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 1);
        }
    }
};

class StringPrimaryKeyTest : public PrimaryKeyTest {
public:
    string getInputDir() override {
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
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 1);
        } else {
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 0);
        }
        tuple = conn->query("MATCH (a:Person)-[e:Knows]->(b:Person) WHERE a.firstStrCol = \"Bob\" "
                            "RETURN COUNT(*)")
                    ->getNext();
        // Edge is from "Alice"->"Bob", and when the primary key is firstStrCol, we expect to find 0
        // result. If key is secondStrCol we expect to find 1 result.
        if (pkColName == "firstStrCol") {
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 0);
        } else {
            ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 1);
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
        personNodeTableID = catalog->getReadOnlyVersion()->getTableID("person");
        organisationTableID = catalog->getReadOnlyVersion()->getTableID("organisation");
        studyAtRelTableID = catalog->getReadOnlyVersion()->getTableID("studyAt");
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = getCatalog(*database);
    }

    string getInputDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }

    void validateDatabaseStateAfterCommitCreateNodeTable() {
        ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("EXAM_PAPER"));
        ASSERT_EQ(getStorageManager(*database)
                      ->getNodesStore()
                      .getNodesStatisticsAndDeletedIDs()
                      .getNumNodeStatisticsAndDeleteIDsPerTable(),
            4);
    }

    // Since DDL statements are in an auto-commit transaction, we can't use the query interface to
    // test the recovery algorithm and parallel read.
    void createNodeTable(TransactionTestType transactionTestType) {
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
            validateDatabaseStateAfterCommitCreateNodeTable();
        } else {
            conn->commit();
            validateDatabaseStateAfterCommitCreateNodeTable();
        }
    }

    void validateDatabaseStateAfterCommitCreateRelTable() {
        ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
        ASSERT_EQ(getStorageManager(*database)->getRelsStore().getNumRelTables(), 7);
    }

    void createRelTable(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            "CREATE REL TABLE likes(FROM person TO person | organisation, RATING INT64, MANY_ONE)");
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(getStorageManager(*database)->getRelsStore().getNumRelTables(), 6);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCommitCreateRelTable();
        } else {
            conn->commit();
            validateDatabaseStateAfterCommitCreateRelTable();
        }
    }

    void validateBelongsRelTable() {
        // Valid relations in belongs table: person->organisation, organisation->country.
        auto result = conn->query("MATCH (:person)-[:belongs]->(:organisation) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(TestHelper::convertResultToString(*result), vector<string>{"2"});
        result = conn->query("MATCH (a:person)-[e:belongs]->(b:country) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Binder exception: Nodes a and b are not connected through rel e.");
        result = conn->query("MATCH (:organisation)-[:belongs]->(:country) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(TestHelper::convertResultToString(*result), vector<string>{"1"});
        result = conn->query("MATCH (a:organisation)-[e:belongs]->(b:person) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Binder exception: Nodes a and b are not connected through rel e.");
        result = conn->query("MATCH (a:country)-[e:belongs]->(b:person) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Binder exception: Nodes a and b are not connected through rel e.");
        result = conn->query("MATCH (a:country)-[e:belongs]->(b:organisation) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Binder exception: Nodes a and b are not connected through rel e.");
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
        auto relTableSchema = (RelTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(
            catalog->getReadOnlyVersion()->getTableID("belongs"));
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
        auto nodeTableSchema = make_unique<NodeTableSchema>(
            *(NodeTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(
                catalog->getReadOnlyVersion()->getTableID("university")));
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
        auto relTableSchema = make_unique<RelTableSchema>(
            *(RelTableSchema*)catalog->getReadOnlyVersion()->getTableSchema(
                catalog->getReadOnlyVersion()->getTableID("knows")));
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

    void validateDatabaseStateAfterCommitDropNodeTableProperty(
        const string& propertyFileName, bool hasOverflowFile, const string& propertyName) {
        validateColumnFilesExistence(propertyFileName, false /* existence */, hasOverflowFile);
        ASSERT_FALSE(catalog->getReadOnlyVersion()
                         ->getTableSchema(personNodeTableID)
                         ->containProperty(propertyName));
        auto result = conn->query("MATCH (p:person) RETURN * ORDER BY p.ID LIMIT 1");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            vector<string>{
                "(0:0:person {ID:0, fName:Alice, isStudent:True, isWorker:False, age:35, "
                "eyeSight:5.000000, birthdate:1900-01-01, registerTime:2011-08-20 11:25:30, "
                "lastJobDuration:3 years 2 days 13:02:00, workedHours:[10,5], "
                "usedNames:[Aida], courseScoresPerTerm:[[10,8],[6,7,8]]})"});
    }

    void dropNodeTableProperty(TransactionTestType transactionTestType) {
        auto propertyToDrop =
            catalog->getReadOnlyVersion()->getNodeProperty(personNodeTableID, "gender");
        auto propertyFileName =
            StorageUtils::getNodePropertyColumnFName(databaseConfig->databasePath,
                personNodeTableID, propertyToDrop.propertyID, DBFileType::ORIGINAL);
        bool hasOverflowFile = containsOverflowFile(propertyToDrop.dataType.typeID);
        executeQueryWithoutCommit("ALTER TABLE person DROP gender");
        validateColumnFilesExistence(propertyFileName, true /* existence */, hasOverflowFile);
        ASSERT_TRUE(catalog->getReadOnlyVersion()
                        ->getTableSchema(personNodeTableID)
                        ->containProperty("gender"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            // The file for property gender should still exist until we do checkpoint.
            validateColumnFilesExistence(propertyFileName, true /* existence */, hasOverflowFile);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCommitDropNodeTableProperty(
                propertyFileName, hasOverflowFile, propertyToDrop.name);
        } else {
            conn->commit();
            validateDatabaseStateAfterCommitDropNodeTableProperty(
                propertyFileName, hasOverflowFile, propertyToDrop.name);
        }
    }

    void validateDatabaseStateAfterCommitDropRelTableProperty(
        const string& propertyFWDColumnFileName, const string& propertyBWDListFileName,
        bool hasOverflowFile, const string& propertyName) {
        validateColumnFilesExistence(
            propertyFWDColumnFileName, false /* existence */, hasOverflowFile);
        validateListFilesExistence(
            propertyBWDListFileName, false /* existence */, hasOverflowFile, false /* hasHeader */);
        ASSERT_FALSE(catalog->getReadOnlyVersion()
                         ->getTableSchema(personNodeTableID)
                         ->containProperty(propertyName));
        auto result = conn->query(
            "MATCH (:person)-[s:studyAt]->(:organisation) RETURN * ORDER BY s.year DESC LIMIT 1");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            vector<string>{"(0:0)-[{_id:14, year:2021}]->(1:0)"});
    }

    void dropRelTableProperty(TransactionTestType transactionTestType) {
        auto propertyToDrop =
            catalog->getReadOnlyVersion()->getRelProperty(studyAtRelTableID, "places");
        // Note: studyAt is a MANY-ONE rel table. Properties are stored as columns in the fwd
        // direction and stored as lists in the bwd direction.
        auto propertyFWDColumnFileName = StorageUtils::getRelPropertyColumnFName(
            databaseConfig->databasePath, studyAtRelTableID, personNodeTableID, RelDirection::FWD,
            propertyToDrop.propertyID, DBFileType::ORIGINAL);
        auto propertyBWDListFileName = StorageUtils::getRelPropertyListsFName(
            databaseConfig->databasePath, studyAtRelTableID, organisationTableID, RelDirection::BWD,
            propertyToDrop.propertyID, DBFileType::ORIGINAL);
        bool hasOverflowFile = containsOverflowFile(propertyToDrop.dataType.typeID);
        executeQueryWithoutCommit("ALTER TABLE studyAt DROP places");
        validateColumnFilesExistence(
            propertyFWDColumnFileName, true /* existence */, hasOverflowFile);
        validateListFilesExistence(
            propertyBWDListFileName, true /* existence */, hasOverflowFile, false /* hasHeader */);
        ASSERT_TRUE(catalog->getReadOnlyVersion()
                        ->getTableSchema(studyAtRelTableID)
                        ->containProperty("places"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            // The file for property places should still exist until we do checkpoint.
            validateColumnFilesExistence(
                propertyFWDColumnFileName, true /* existence */, hasOverflowFile);
            validateListFilesExistence(propertyBWDListFileName, true /* existence */,
                hasOverflowFile, false /* hasHeader */);
            initWithoutLoadingGraph();
            validateDatabaseStateAfterCommitDropRelTableProperty(propertyFWDColumnFileName,
                propertyBWDListFileName, hasOverflowFile, propertyToDrop.name);
        } else {
            conn->commit();
            validateDatabaseStateAfterCommitDropRelTableProperty(propertyFWDColumnFileName,
                propertyBWDListFileName, hasOverflowFile, propertyToDrop.name);
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
        auto physicalPlan =
            mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[0].get(),
                preparedStatement->getExpressionsToCollect(), preparedStatement->statementType);
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
    }

    void addPropertyToPersonTableWithoutDefaultValue(string propertyType) {
        ASSERT_TRUE(
            conn->query(StringUtils::string_format(
                            "ALTER TABLE person ADD COLUMN random %s", propertyType.c_str()))
                ->isSuccess());
        auto result = conn->query(StringUtils::string_format("MATCH (p:person) return p.random"));
        while (result->hasNext()) {
            ASSERT_TRUE(result->getNext()->getValue(0 /* idx */)->isNull());
        }
    }

    void addPropertyToKnowsTableWithoutDefaultValue(string propertyType) {
        ASSERT_TRUE(conn->query(StringUtils::string_format(
                                    "ALTER TABLE knows ADD COLUMN random %s", propertyType.c_str()))
                        ->isSuccess());
        auto result = conn->query(
            StringUtils::string_format("MATCH (:person)-[e:knows]->(:person) return e.random"));
        while (result->hasNext()) {
            ASSERT_TRUE(result->getNext()->getValue(0 /* idx */)->isNull());
        }
    }

    Catalog* catalog;
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<ExecutionContext> executionContext;
    unique_ptr<Profiler> profiler;
    table_id_t personNodeTableID;
    table_id_t organisationTableID;
    table_id_t studyAtRelTableID;
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

TEST_F(TinySnbDDLTest, DDLStatementWithActiveTransactionError) {
    ddlStatementsInsideActiveTransactionErrorTest(
        "CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
        "STRING, REGISTER_TIME DATE, PRIMARY KEY (NAME))");
    ddlStatementsInsideActiveTransactionErrorTest("DROP TABLE knows");
    ddlStatementsInsideActiveTransactionErrorTest("ALTER TABLE person DROP gender");
}

TEST_F(TinySnbDDLTest, CreateNodeTableCommitNormalExecution) {
    createNodeTable(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateNodeTableCommitRecovery) {
    createNodeTable(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, CreateRelTableCommitNormalExecution) {
    createRelTable(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateRelTableCommitRecovery) {
    createRelTable(TransactionTestType::RECOVERY);
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
    result = conn->query("ALTER TABLE person DROP fName;");
    ASSERT_EQ(TestHelper::convertResultToString(*result), vector<string>{"Drop succeed."});
    result = conn->query("ALTER TABLE knows DROP date;");
    ASSERT_EQ(TestHelper::convertResultToString(*result), vector<string>{"Drop succeed."});
}

TEST_F(TinySnbDDLTest, CreateMixedRelationTableNormalExecution) {
    createRelMixedRelationCommitAndRecoveryTest(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, CreateMixedRelationTableRecovery) {
    createRelMixedRelationCommitAndRecoveryTest(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DropNodeTablePropertyNormalExecution) {
    dropNodeTableProperty(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, DropNodeTablePropertyRecovery) {
    dropNodeTableProperty(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DropRelTablePropertyNormalExecution) {
    dropRelTableProperty(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, DropRelTablePropertyRecovery) {
    dropRelTableProperty(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToNodeTableWithoutDefaultValue) {
    addPropertyToPersonTableWithoutDefaultValue("INT64");
}

TEST_F(TinySnbDDLTest, AddStringPropertyToNodeTableWithoutDefaultValue) {
    addPropertyToPersonTableWithoutDefaultValue("STRING");
}

TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToNodeTableWithoutDefaultValue) {
    addPropertyToPersonTableWithoutDefaultValue("INT64[]");
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToNodeTableWithoutDefaultValue) {
    addPropertyToPersonTableWithoutDefaultValue("STRING[]");
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToNodeTableWithDefaultValue) {
    ASSERT_TRUE(conn->query("ALTER TABLE person ADD COLUMN random INT64 DEFAULT 57")->isSuccess());
    vector<string> expectedResult(8, "57");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToNodeTableWithDefaultValue) {
    ASSERT_TRUE(
        conn->query(
                "ALTER TABLE person ADD COLUMN random STRING DEFAULT 'this is a long string!!!'")
            ->isSuccess());
    vector<string> expectedResult(8, "this is a long string!!!");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToNodeTableWithDefaultValue) {
    ASSERT_TRUE(conn->query("ALTER TABLE person ADD COLUMN random INT64[] DEFAULT [142, 123, 789]")
                    ->isSuccess());
    vector<string> expectedResult(8, "[142,123,789]");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToNodeTableWithDefaultValue) {
    ASSERT_TRUE(conn->query("ALTER TABLE person ADD COLUMN random STRING[] DEFAULT ['142', "
                            "'short', 'long long long string']")
                    ->isSuccess());
    vector<string> expectedResult(8, "[142,short,long long long string]");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddListOfListOfStringPropertyToNodeTableWithDefaultValue) {
    ASSERT_TRUE(
        conn->query("ALTER TABLE person ADD COLUMN random STRING[][] DEFAULT [['142', '341'], "
                    "['short'], ['long long long string', '132321412421421414214']]")
            ->isSuccess());
    vector<string> expectedResult(
        8, "[[142,341],[short],[long long long string,132321412421421414214]]");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToRelTableWithoutDefaultValue) {
    addPropertyToKnowsTableWithoutDefaultValue("INT64");
}

TEST_F(TinySnbDDLTest, AddStringPropertyToRelTableWithoutDefaultValue) {
    addPropertyToKnowsTableWithoutDefaultValue("STRING");
}

TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToRelTableWithoutDefaultValue) {
    addPropertyToKnowsTableWithoutDefaultValue("INT64[]");
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToRelTableWithoutDefaultValue) {
    addPropertyToKnowsTableWithoutDefaultValue("STRING[]");
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToManyManyRelTableWithDefaultValue) {
    ASSERT_TRUE(conn->query("ALTER TABLE knows ADD COLUMN random INT64 DEFAULT 23")->isSuccess());
    vector<string> expectedResult(14, "23");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (:person)-[e:knows]->(:person) return e.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToManyManyRelTableWithDefaultValue) {
    ASSERT_TRUE(
        conn->query("ALTER TABLE knows ADD COLUMN random STRING DEFAULT 'very very long long'")
            ->isSuccess());
    vector<string> expectedResult(14, "very very long long");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (:person)-[e:knows]->(:person) return e.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToManyManyRelTableWithDefaultValue) {
    ASSERT_TRUE(
        conn->query("ALTER TABLE knows ADD COLUMN random INT64[] DEFAULT [7,8,9]")->isSuccess());
    vector<string> expectedResult(14, "[7,8,9]");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (:person)-[e:knows]->(:person) return e.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToManyManyRelTableWithDefaultValue) {
    ASSERT_TRUE(
        conn->query(
                "ALTER TABLE knows ADD COLUMN random STRING[] DEFAULT ['7','long str long str']")
            ->isSuccess());
    vector<string> expectedResult(14, "[7,long str long str]");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (:person)-[e:knows]->(:person) return e.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddListOfListOfStringPropertyToManyManyRelTableWithDefaultValue) {
    ASSERT_TRUE(conn->query("ALTER TABLE knows ADD COLUMN random STRING[][] DEFAULT [['7', 'very "
                            "very long long'],['long str long str', 'short']]")
                    ->isSuccess());
    vector<string> expectedResult(14, "[[7,very very long long],[long str long str,short]]");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (:person)-[e:knows]->(:person) return e.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToManyOneRelTableWithDefaultValue) {
    ASSERT_TRUE(conn->query("ALTER TABLE studyAt ADD COLUMN random INT64 DEFAULT 8")->isSuccess());
    vector<string> expectedResult(3, "8");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (:person)-[e:studyAt]->(:organisation) return e.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToOneOneRelTableWithDefaultValue) {
    ASSERT_TRUE(conn->query("ALTER TABLE marries ADD COLUMN random INT64 DEFAULT 12")->isSuccess());
    vector<string> expectedResult(3, "12");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (:person)-[e:marries]->(:person) return e.random")),
        expectedResult);
}

TEST_F(TinySnbDDLTest, AddPropertyWithComplexExpression) {
    ASSERT_TRUE(conn->query("ALTER TABLE person ADD COLUMN random INT64 DEFAULT  2 * abs(-2)")
                    ->isSuccess());
    vector<string> expectedResult(8, "4");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
        expectedResult);
}
