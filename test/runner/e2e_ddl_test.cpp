#include "common/string_utils.h"
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

class PrimaryKeyTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
    }
};

class IntPrimaryKeyTest : public PrimaryKeyTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/primary-key-tests/int-pk-tests/");
    }

    void testPrimaryKey(std::string pkColName) {
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
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/primary-key-tests/string-pk-tests/");
    }

    void testPrimaryKey(std::string pkColName) {
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
        profiler = std::make_unique<Profiler>();
        bufferManager = std::make_unique<BufferManager>(
            BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = std::make_unique<MemoryManager>(bufferManager.get());
        clientContext = std::make_unique<ClientContext>();
        executionContext = std::make_unique<ExecutionContext>(1 /* numThreads */, profiler.get(),
            memoryManager.get(), bufferManager.get(), clientContext.get());
        personTableID = catalog->getReadOnlyVersion()->getTableID("person");
        studyAtTableID = catalog->getReadOnlyVersion()->getTableID("studyAt");
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = getCatalog(*database);
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

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
        } else {
            conn->commit();
        }
        validateDatabaseStateAfterCommitCreateNodeTable();
    }

    void validateDatabaseStateAfterCommitCreateRelTable() {
        ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("likes"));
        ASSERT_EQ(getStorageManager(*database)->getRelsStore().getNumRelTables(), 6);
    }

    void createRelTable(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            "CREATE REL TABLE likes(FROM person TO organisation, RATING INT64, MANY_ONE)");
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
            ASSERT_EQ(getStorageManager(*database)->getRelsStore().getNumRelTables(), 5);
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        validateDatabaseStateAfterCommitCreateRelTable();
    }

    void validateBelongsRelTable() {
        // Valid relations in belongs table: person->organisation, organisation->country.
        auto result = conn->query("MATCH (:person)-[:belongs]->(:organisation) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(TestHelper::convertResultToString(*result), std::vector<std::string>{"2"});
        result = conn->query("MATCH (a:person)-[e:belongs]->(b:country) RETURN count(*)");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Binder exception: Nodes a and b are not connected through rel e.");
        result = conn->query("MATCH (:organisation)-[:belongs]->(:country) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(TestHelper::convertResultToString(*result), std::vector<std::string>{"1"});
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
        } else {
            conn->commit();
        }
        validateBelongsRelTable();
    }

    void dropNodeTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        conn->query("CREATE NODE TABLE university(address STRING, PRIMARY KEY(address));");
        auto nodeTableSchema = std::make_unique<NodeTableSchema>(
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
        } else {
            conn->commit();
        }
        validateNodeColumnFilesExistence(nodeTableSchema.get(), DBFileType::ORIGINAL, false);
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("university"));
    }

    void dropRelTableCommitAndRecoveryTest(TransactionTestType transactionTestType) {
        auto relTableSchema = std::make_unique<RelTableSchema>(
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
        } else {
            conn->commit();
        }
        validateRelColumnAndListFilesExistence(relTableSchema.get(), DBFileType::ORIGINAL, false);
        ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("knows"));
    }

    void dropNodeTableProperty(TransactionTestType transactionTestType) {
        auto propertyToDrop =
            catalog->getReadOnlyVersion()->getNodeProperty(personTableID, "gender");
        auto propertyFileName = StorageUtils::getNodePropertyColumnFName(
            databasePath, personTableID, propertyToDrop.propertyID, DBFileType::ORIGINAL);
        bool hasOverflowFile = containsOverflowFile(propertyToDrop.dataType.getLogicalTypeID());
        executeQueryWithoutCommit("ALTER TABLE person DROP gender");
        validateColumnFilesExistence(propertyFileName, true /* existence */, hasOverflowFile);
        ASSERT_TRUE(catalog->getReadOnlyVersion()
                        ->getTableSchema(personTableID)
                        ->containProperty("gender"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            // The file for property gender should still exist until we do checkpoint.
            validateColumnFilesExistence(propertyFileName, true /* existence */, hasOverflowFile);
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        validateColumnFilesExistence(propertyFileName, false /* existence */, hasOverflowFile);
        ASSERT_FALSE(catalog->getReadOnlyVersion()
                         ->getTableSchema(personTableID)
                         ->containProperty("gender"));
        auto result = conn->query("MATCH (p:person) RETURN * ORDER BY p.ID LIMIT 1");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>{
                "(label:person, 0:0, {ID:0, fName:Alice, isStudent:True, isWorker:False, age:35, "
                "eyeSight:5.000000, birthdate:1900-01-01, registerTime:2011-08-20 11:25:30, "
                "lastJobDuration:3 years 2 days 13:02:00, workedHours:[10,5], "
                "usedNames:[Aida], courseScoresPerTerm:[[10,8],[6,7,8]], grades:[96,54,86,92], "
                "height:1.731000})"});
    }

    void dropRelTableProperty(TransactionTestType transactionTestType) {
        auto propertyToDrop =
            catalog->getReadOnlyVersion()->getRelProperty(studyAtTableID, "places");
        // Note: studyAt is a MANY-ONE rel table. Properties are stored as columns in the fwd
        // direction and stored as lists in the bwd direction.
        auto propertyFWDColumnFileName = StorageUtils::getRelPropertyColumnFName(databasePath,
            studyAtTableID, RelDataDirection::FWD, propertyToDrop.propertyID, DBFileType::ORIGINAL);
        auto propertyBWDListFileName = StorageUtils::getRelPropertyListsFName(databasePath,
            studyAtTableID, RelDataDirection::BWD, propertyToDrop.propertyID, DBFileType::ORIGINAL);
        bool hasOverflowFile = containsOverflowFile(propertyToDrop.dataType.getLogicalTypeID());
        executeQueryWithoutCommit("ALTER TABLE studyAt DROP places");
        validateColumnFilesExistence(
            propertyFWDColumnFileName, true /* existence */, hasOverflowFile);
        validateListFilesExistence(
            propertyBWDListFileName, true /* existence */, hasOverflowFile, false /* hasHeader */);
        ASSERT_TRUE(catalog->getReadOnlyVersion()
                        ->getTableSchema(studyAtTableID)
                        ->containProperty("places"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            // The file for property places should still exist until we do checkpoint.
            validateColumnFilesExistence(
                propertyFWDColumnFileName, true /* existence */, hasOverflowFile);
            validateListFilesExistence(propertyBWDListFileName, true /* existence */,
                hasOverflowFile, false /* hasHeader */);
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        validateColumnFilesExistence(
            propertyFWDColumnFileName, false /* existence */, hasOverflowFile);
        validateListFilesExistence(
            propertyBWDListFileName, false /* existence */, hasOverflowFile, false /* hasHeader */);
        ASSERT_FALSE(catalog->getReadOnlyVersion()
                         ->getTableSchema(personTableID)
                         ->containProperty("places"));
        auto result = conn->query(
            "MATCH (:person)-[s:studyAt]->(:organisation) RETURN * ORDER BY s.year DESC LIMIT 1");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>{
                "(0:0)-[label:studyAt, {_id:4:0, year:2021, length:5}]->(1:0)"});
    }

    void ddlStatementsInsideActiveTransactionErrorTest(std::string query) {
        conn->beginWriteTransaction();
        auto result = conn->query(query);
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "DDL and CopyCSV statements are automatically wrapped in a transaction and committed. "
            "As such, they cannot be part of an active transaction, please commit or rollback your "
            "previous transaction and issue a ddl query without opening a transaction.");
    }

    void executeQueryWithoutCommit(std::string query) {
        auto preparedStatement = conn->prepare(query);
        conn->beginWriteTransaction();
        auto mapper = PlanMapper(
            *getStorageManager(*database), getMemoryManager(*database), getCatalog(*database));
        auto physicalPlan = mapper.mapLogicalPlanToPhysical(
            preparedStatement->logicalPlans[0].get(), preparedStatement->getExpressionsToCollect());
        executionContext->clientContext->activeQuery = std::make_unique<ActiveQuery>();
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
    }

    void validateDatabaseFileBeforeCheckpointAddProperty(const std::string& originalVersionFileName,
        const std::string& walVersionFileName, bool hasOverflow) {
        validateColumnFilesExistence(walVersionFileName, true /* existence */, hasOverflow);
        validateColumnFilesExistence(originalVersionFileName, false /* existence */, hasOverflow);
    }

    void validateDatabaseFileAfterCheckpointAddProperty(const std::string& originalVersionFileName,
        const std::string& walVersionFileName, bool hasOverflow) {
        validateColumnFilesExistence(walVersionFileName, false /* existence */, hasOverflow);
        validateColumnFilesExistence(originalVersionFileName, true /* existence */, hasOverflow);
    }

    void addPropertyToPersonTableWithoutDefaultValue(
        std::string propertyType, TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            StringUtils::string_format("ALTER TABLE person ADD random {}", propertyType));
        auto tableSchema = catalog->getWriteVersion()->getTableSchema(personTableID);
        auto propertyID = tableSchema->getPropertyID("random");
        auto hasOverflow =
            containsOverflowFile(tableSchema->getProperty(propertyID).dataType.getLogicalTypeID());
        auto columnOriginalVersionFileName = StorageUtils::getNodePropertyColumnFName(
            databasePath, personTableID, propertyID, DBFileType::ORIGINAL);
        auto columnWALVersionFileName = StorageUtils::getNodePropertyColumnFName(
            databasePath, personTableID, propertyID, DBFileType::WAL_VERSION);
        validateDatabaseFileBeforeCheckpointAddProperty(
            columnOriginalVersionFileName, columnWALVersionFileName, hasOverflow);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateDatabaseFileBeforeCheckpointAddProperty(
                columnOriginalVersionFileName, columnWALVersionFileName, hasOverflow);
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        validateDatabaseFileAfterCheckpointAddProperty(
            columnOriginalVersionFileName, columnWALVersionFileName, hasOverflow);
        // The default value of the property is NULL if not specified by the user.
        auto result = conn->query("MATCH (p:person) return p.random");
        while (result->hasNext()) {
            ASSERT_TRUE(result->getNext()->getValue(0 /* idx */)->isNull());
        }
    }

    void addPropertyToPersonTableWithDefaultValue(
        std::string propertyType, std::string defaultVal, TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            "ALTER TABLE person ADD random " + propertyType + " DEFAULT " + defaultVal);
        auto tableSchema = catalog->getWriteVersion()->getTableSchema(personTableID);
        auto propertyID = tableSchema->getPropertyID("random");
        auto hasOverflow =
            containsOverflowFile(tableSchema->getProperty(propertyID).dataType.getLogicalTypeID());
        auto columnOriginalVersionFileName = StorageUtils::getNodePropertyColumnFName(
            databasePath, personTableID, propertyID, DBFileType::ORIGINAL);
        auto columnWALVersionFileName = StorageUtils::getNodePropertyColumnFName(
            databasePath, personTableID, propertyID, DBFileType::WAL_VERSION);
        validateDatabaseFileBeforeCheckpointAddProperty(
            columnOriginalVersionFileName, columnWALVersionFileName, hasOverflow);
        // The convertResultToString function will remove the single quote around the result
        // std::string, so we should also remove the single quote in the expected result.
        defaultVal.erase(remove(defaultVal.begin(), defaultVal.end(), '\''), defaultVal.end());
        std::vector<std::string> expectedResult(8 /* numOfNodesInPesron */, defaultVal);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateDatabaseFileBeforeCheckpointAddProperty(
                columnOriginalVersionFileName, columnWALVersionFileName, hasOverflow);
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        validateDatabaseFileAfterCheckpointAddProperty(
            columnOriginalVersionFileName, columnWALVersionFileName, hasOverflow);
        ASSERT_EQ(
            TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
            expectedResult);
    }

    void addPropertyToStudyAtTableWithoutDefaultValue(
        std::string propertyType, TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            StringUtils::string_format("ALTER TABLE studyAt ADD random {}", propertyType));
        auto tableSchema = catalog->getWriteVersion()->getTableSchema(studyAtTableID);
        auto propertyID = tableSchema->getPropertyID("random");
        auto hasOverflow =
            containsOverflowFile(tableSchema->getProperty(propertyID).dataType.getLogicalTypeID());
        auto fwdColumnOriginalVersionFileName = StorageUtils::getRelPropertyColumnFName(
            databasePath, studyAtTableID, RelDataDirection::FWD, propertyID, DBFileType::ORIGINAL);
        auto fwdColumnWALVersionFileName = StorageUtils::getRelPropertyColumnFName(databasePath,
            studyAtTableID, RelDataDirection::FWD, propertyID, DBFileType::WAL_VERSION);
        auto bwdListOriginalVersionFileName = StorageUtils::getRelPropertyListsFName(
            databasePath, studyAtTableID, RelDataDirection::BWD, propertyID, DBFileType::ORIGINAL);
        auto bwdListWALVersionFileName = StorageUtils::getRelPropertyListsFName(databasePath,
            studyAtTableID, RelDataDirection::BWD, propertyID, DBFileType::WAL_VERSION);
        validateDatabaseFileBeforeCheckpointAddProperty(
            fwdColumnOriginalVersionFileName, fwdColumnWALVersionFileName, hasOverflow);
        validateDatabaseFileBeforeCheckpointAddProperty(
            bwdListOriginalVersionFileName, bwdListWALVersionFileName, hasOverflow);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateDatabaseFileBeforeCheckpointAddProperty(
                fwdColumnOriginalVersionFileName, fwdColumnWALVersionFileName, hasOverflow);
            validateDatabaseFileBeforeCheckpointAddProperty(
                bwdListOriginalVersionFileName, bwdListWALVersionFileName, hasOverflow);
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        validateDatabaseFileAfterCheckpointAddProperty(
            fwdColumnOriginalVersionFileName, fwdColumnWALVersionFileName, hasOverflow);
        validateDatabaseFileAfterCheckpointAddProperty(
            bwdListOriginalVersionFileName, bwdListWALVersionFileName, hasOverflow);
        // Note: the default value of the new property is NULL if not specified by the user.
        auto result = conn->query("MATCH (:person)-[e:studyAt]->(:organisation) return e.random");
        while (result->hasNext()) {
            ASSERT_TRUE(result->getNext()->getValue(0 /* idx */)->isNull());
        }
    }

    void addPropertyToStudyAtTableWithDefaultValue(
        std::string propertyType, std::string defaultVal, TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(StringUtils::string_format(
            "ALTER TABLE studyAt ADD random {} DEFAULT {}", propertyType, defaultVal));
        auto relTableSchema = catalog->getWriteVersion()->getTableSchema(studyAtTableID);
        auto propertyID = relTableSchema->getPropertyID("random");
        auto hasOverflow = containsOverflowFile(
            relTableSchema->getProperty(propertyID).dataType.getLogicalTypeID());
        auto fwdColumnOriginalVersionFileName = StorageUtils::getRelPropertyColumnFName(
            databasePath, studyAtTableID, RelDataDirection::FWD, propertyID, DBFileType::ORIGINAL);
        auto fwdColumnWALVersionFileName = StorageUtils::getRelPropertyColumnFName(databasePath,
            studyAtTableID, RelDataDirection::FWD, propertyID, DBFileType::WAL_VERSION);
        auto bwdListOriginalVersionFileName = StorageUtils::getRelPropertyListsFName(
            databasePath, studyAtTableID, RelDataDirection::BWD, propertyID, DBFileType::ORIGINAL);
        auto bwdListWALVersionFileName = StorageUtils::getRelPropertyListsFName(databasePath,
            studyAtTableID, RelDataDirection::BWD, propertyID, DBFileType::WAL_VERSION);
        validateDatabaseFileBeforeCheckpointAddProperty(
            fwdColumnOriginalVersionFileName, fwdColumnWALVersionFileName, hasOverflow);
        validateDatabaseFileBeforeCheckpointAddProperty(
            bwdListOriginalVersionFileName, bwdListWALVersionFileName, hasOverflow);
        defaultVal.erase(remove(defaultVal.begin(), defaultVal.end(), '\''), defaultVal.end());
        std::vector<std::string> expectedResult(3 /* numOfRelsInStudyAt */, defaultVal);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            validateDatabaseFileBeforeCheckpointAddProperty(
                fwdColumnOriginalVersionFileName, fwdColumnWALVersionFileName, hasOverflow);
            validateDatabaseFileBeforeCheckpointAddProperty(
                bwdListOriginalVersionFileName, bwdListWALVersionFileName, hasOverflow);
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        validateDatabaseFileAfterCheckpointAddProperty(
            fwdColumnOriginalVersionFileName, fwdColumnWALVersionFileName, hasOverflow);
        validateDatabaseFileAfterCheckpointAddProperty(
            bwdListOriginalVersionFileName, bwdListWALVersionFileName, hasOverflow);
        ASSERT_EQ(TestHelper::convertResultToString(
                      *conn->query("MATCH (:person)-[e:studyAt]->(:organisation) return e.random")),
            expectedResult);
    }

    void renameTable(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit("ALTER TABLE person RENAME TO student");
        ASSERT_EQ(catalog->getWriteVersion()->getTableSchema(personTableID)->tableName, "student");
        ASSERT_EQ(
            catalog->getReadOnlyVersion()->getTableSchema(personTableID)->tableName, "person");
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            ASSERT_EQ(
                catalog->getWriteVersion()->getTableSchema(personTableID)->tableName, "student");
            ASSERT_EQ(
                catalog->getReadOnlyVersion()->getTableSchema(personTableID)->tableName, "person");
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        ASSERT_EQ(
            catalog->getReadOnlyVersion()->getTableSchema(personTableID)->tableName, "student");
        auto result = conn->query("MATCH (s:student) return s.age order by s.age");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>({"20", "20", "25", "30", "35", "40", "45", "83"}));
    }

    void renameProperty(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit("ALTER TABLE person RENAME fName TO name");
        ASSERT_TRUE(
            catalog->getWriteVersion()->getTableSchema(personTableID)->containProperty("name"));
        ASSERT_TRUE(
            catalog->getReadOnlyVersion()->getTableSchema(personTableID)->containProperty("fName"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            commitButSkipCheckpointingForTestingRecovery(*conn);
            ASSERT_TRUE(
                catalog->getWriteVersion()->getTableSchema(personTableID)->containProperty("name"));
            ASSERT_TRUE(catalog->getReadOnlyVersion()
                            ->getTableSchema(personTableID)
                            ->containProperty("fName"));
            initWithoutLoadingGraph();
        } else {
            conn->commit();
        }
        ASSERT_TRUE(
            catalog->getReadOnlyVersion()->getTableSchema(personTableID)->containProperty("name"));
        auto result = conn->query("MATCH (p:person) return p.name order by p.name");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>({"Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg",
                "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"}));
    }

    Catalog* catalog;
    std::unique_ptr<BufferManager> bufferManager;
    std::unique_ptr<MemoryManager> memoryManager;
    std::unique_ptr<ExecutionContext> executionContext;
    std::unique_ptr<ClientContext> clientContext;
    std::unique_ptr<Profiler> profiler;
    table_id_t personTableID;
    table_id_t studyAtTableID;
};

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
        std::vector<std::string>{"NodeTable: university has been created."});
    result = conn->query("CREATE REL TABLE nearTo(FROM university TO university, MANY_MANY);");
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        std::vector<std::string>{"RelTable: nearTo has been created."});
    result = conn->query("DROP TABLE nearTo;");
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        std::vector<std::string>{"RelTable: nearTo has been dropped."});
    result = conn->query("DROP TABLE university;");
    ASSERT_EQ(TestHelper::convertResultToString(*result),
        std::vector<std::string>{"NodeTable: university has been dropped."});
    result = conn->query("ALTER TABLE person DROP fName;");
    ASSERT_EQ(
        TestHelper::convertResultToString(*result), std::vector<std::string>{"Drop succeed."});
    result = conn->query("ALTER TABLE knows DROP date;");
    ASSERT_EQ(
        TestHelper::convertResultToString(*result), std::vector<std::string>{"Drop succeed."});
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

TEST_F(TinySnbDDLTest, AddInt64PropertyToPersonTableWithoutDefaultValueNormalExecution) {
    addPropertyToPersonTableWithoutDefaultValue(
        "INT64" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "INT64" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddFixListPropertyToPersonTableWithoutDefaultValueNormalExecution) {
    addPropertyToPersonTableWithoutDefaultValue(
        "INT64[3]" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, AddFixedListPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "DOUBLE[5]" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToPersonTableWithoutDefaultValueNormalExecution) {
    addPropertyToPersonTableWithoutDefaultValue(
        "STRING" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "STRING" /* propertyType */, TransactionTestType::RECOVERY);
}

// TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToPersonTableWithoutDefaultValueNormalExecution) {
//    addPropertyToPersonTableWithoutDefaultValue(
//        "INT64[]" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToPersonTableWithoutDefaultValueRecovery) {
//    addPropertyToPersonTableWithoutDefaultValue(
//        "INT64[]" /* propertyType */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToPersonTableWithoutDefaultValueNormalExecution) {
//    addPropertyToPersonTableWithoutDefaultValue(
//        "STRING[]" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToPersonTableWithoutDefaultValueRecovery) {
//    addPropertyToPersonTableWithoutDefaultValue(
//        "STRING[]" /* propertyType */, TransactionTestType::RECOVERY);
//}

// TEST_F(TinySnbDDLTest, AddInt64PropertyToPersonTableWithDefaultValueNormalExecution) {
//    addPropertyToPersonTableWithDefaultValue(
//        "INT64" /* propertyType */, "8" /* defaultVal */, TransactionTestType::NORMAL_EXECUTION);
//}

// TEST_F(TinySnbDDLTest, AddInt64PropertyToPersonTableWithDefaultValueRecovery) {
//    addPropertyToPersonTableWithDefaultValue(
//        "INT64" /* propertyType */, "21" /* defaultVal */, TransactionTestType::RECOVERY);
//}

// TEST_F(TinySnbDDLTest, AddStringPropertyToPersonTableWithDefaultValueNormalExecution) {
//    addPropertyToPersonTableWithDefaultValue("STRING" /* propertyType */,
//        "'long long string'" /* defaultVal */, TransactionTestType::NORMAL_EXECUTION);
//}

// TEST_F(TinySnbDDLTest, AddStringPropertyToPersonTableWithDefaultValueRecovery) {
//    addPropertyToPersonTableWithDefaultValue("STRING" /* propertyType */,
//        "'long long string'" /* defaultVal */, TransactionTestType::RECOVERY);
//}

// TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToPersonTableWithDefaultValueNormalExecution) {
//    addPropertyToPersonTableWithDefaultValue("INT64[]" /* propertyType */,
//        "[142,123,789]" /* defaultVal */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToPersonTableWithDefaultValueRecovery) {
//    addPropertyToPersonTableWithDefaultValue("INT64[]" /* propertyType */,
//        "[142,123,789]" /* defaultVal */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToPersonTableWithDefaultValueNormalExecution) {
//    addPropertyToPersonTableWithDefaultValue("STRING[]" /* propertyType */,
//        "['142','short','long long long string']" /* defaultValue */,
//        TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToPersonTableWithDefaultValueRecovery) {
//    addPropertyToPersonTableWithDefaultValue("STRING[]" /* propertyType */,
//        "['142','short','long long long string']" /* defaultValue */,
//        TransactionTestType::RECOVERY);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfListOfStringPropertyToPersonTableWithDefaultValueNormalExecution)
// {
//    addPropertyToPersonTableWithDefaultValue("STRING[][]" /* propertyType */,
//        "[['142','51'],['short','long','123'],['long long long string','short short short short "
//        "short']]" /* defaultValue */,
//        TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfListOfStringPropertyToPersonTableWithDefaultValueRecovery) {
//    addPropertyToPersonTableWithDefaultValue("STRING[][]" /* propertyType */,
//        "[['142','51'],['short','long','123'],['long long long string','short short short short',"
//        "'short']]" /* defaultValue */,
//        TransactionTestType::RECOVERY);
//}

TEST_F(TinySnbDDLTest, AddInt64PropertyToStudyAtTableWithoutDefaultValueNormalExecution) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "INT64" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToStudyAtTableWithoutDefaultValueRecovery) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "INT64" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToStudyAtTableWithoutDefaultValueNormalExecution) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "STRING" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToStudyAtTableWithoutDefaultValueRecovery) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "STRING" /* propertyType */, TransactionTestType::RECOVERY);
}

// TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToStudyAtTableWithoutDefaultValueNormalExecution) {
//    addPropertyToStudyAtTableWithoutDefaultValue(
//        "INT64[]" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToStudyAtTableWithoutDefaultValueRecovery) {
//    addPropertyToStudyAtTableWithoutDefaultValue(
//        "INT64[]" /* propertyType */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToStudyAtTableWithoutDefaultValueNormalExecution) {
//    addPropertyToStudyAtTableWithoutDefaultValue(
//        "STRING[]" /* propertyType */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToStudyAtTableWithoutDefaultValueRecovery) {
//    addPropertyToStudyAtTableWithoutDefaultValue(
//        "STRING[]" /* propertyType */, TransactionTestType::RECOVERY);
//}

// TEST_F(TinySnbDDLTest, AddInt64PropertyToStudyAtTableWithDefaultValueNormalExecution) {
//    addPropertyToStudyAtTableWithDefaultValue(
//        "INT64" /* propertyType */, "42" /* defaultVal */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddInt64PropertyToStudyAtTableWithDefaultValueRecovery) {
//    addPropertyToStudyAtTableWithDefaultValue(
//        "INT64" /* propertyType */, "42" /* defaultVal */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(TinySnbDDLTest, AddStringPropertyToStudyAtTableWithDefaultValueNormalExecution) {
//    addPropertyToStudyAtTableWithDefaultValue("STRING" /* propertyType */,
//        "'VERY LONG STRING!!'" /* defaultVal */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddStringPropertyToStudyAtTableWithDefaultValueRecovery) {
//    addPropertyToStudyAtTableWithDefaultValue("STRING" /* propertyType */,
//        "'VERY SHORT STRING!!'" /* defaultVal */, TransactionTestType::RECOVERY);
//}

// TEST_F(TinySnbDDLTest, AddListOfINT64PropertyToStudyAtTableWithDefaultValueNormalExecution) {
//    addPropertyToStudyAtTableWithDefaultValue("INT64[]" /* propertyType */,
//        "[11,15,20]" /* defaultVal */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfINT64PropertyToStudyAtTableWithDefaultValueRecovery) {
//    addPropertyToStudyAtTableWithDefaultValue("INT64[]" /* propertyType */,
//        "[5,6,7,1,3]" /* defaultVal */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToStudyAtTableWithDefaultValueNormalExecution) {
//    addPropertyToStudyAtTableWithDefaultValue("STRING[]" /* propertyType */,
//        "['13','15','long string!!']" /* defaultVal */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfStringPropertyToStudyAtTableWithDefaultValueRecovery) {
//    addPropertyToStudyAtTableWithDefaultValue("STRING[]" /* propertyType */,
//        "['2','SHORT','SUPER LONG STRINGS']" /* defaultVal */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(TinySnbDDLTest,
// AddListOfListOfStringPropertyToStudyAtTableWithDefaultValueNormalExecution) {
//    addPropertyToStudyAtTableWithDefaultValue("STRING[][]" /* propertyType */,
//        "[['hello','good','long long string test'],['6'],['very very long string']]" /* defaultVal
//                                                                                      */
//        ,
//        TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(TinySnbDDLTest, AddListOfListOfStringPropertyToStudyAtTableWithDefaultValueRecovery) {
//    addPropertyToStudyAtTableWithDefaultValue("STRING[][]" /* propertyType */,
//        "[['hello','good','long long string test'],['6'],['very very long string']]" /* defaultVal
//                                                                                      */
//        ,
//        TransactionTestType::RECOVERY);
//}

// TEST_F(TinySnbDDLTest, AddPropertyWithComplexExpression) {
//    ASSERT_TRUE(
//        conn->query("ALTER TABLE person ADD random INT64 DEFAULT  2 * abs(-2)")->isSuccess());
//    std::vector<std::string> expectedResult(8, "4");
//    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
//        expectedResult);
//}

TEST_F(TinySnbDDLTest, RenameTableNormalExecution) {
    renameTable(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, RenameTableRecovery) {
    renameTable(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, RenamePropertyNormalExecution) {
    renameProperty(TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(TinySnbDDLTest, RenamePropertyRecovery) {
    renameProperty(TransactionTestType::RECOVERY);
}

} // namespace testing
} // namespace kuzu
