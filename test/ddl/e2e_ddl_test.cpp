#include "binder/bound_statement_result.h"
#include "common/string_format.h"
#include "graph_test/graph_test.h"
#include "processor/plan_mapper.h"
#include "processor/processor.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::testing;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

class TinySnbDDLTest : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        catalog = getCatalog(*database);
        profiler = std::make_unique<Profiler>();
        bufferManager = std::make_unique<BufferManager>(
            BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING,
            BufferPoolConstants::DEFAULT_VM_REGION_MAX_SIZE);
        memoryManager =
            std::make_unique<MemoryManager>(bufferManager.get(), getFileSystem(*database));
        executionContext =
            std::make_unique<ExecutionContext>(profiler.get(), conn->clientContext.get());
        personTableID = catalog->getTableID(&DUMMY_READ_TRANSACTION, "person");
        studyAtTableID = catalog->getTableID(&DUMMY_READ_TRANSACTION, "studyAt");
    }

    void initWithoutLoadingGraph() {
        createDBAndConn();
        catalog = getCatalog(*database);
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    void validateDatabaseStateAfterCommitCreateNodeTable() {
        ASSERT_TRUE(catalog->containsTable(&DUMMY_READ_TRANSACTION, "EXAM_PAPER"));
        ASSERT_EQ(getStorageManager(*database)
                      ->getNodesStatisticsAndDeletedIDs()
                      ->getNumNodeStatisticsAndDeleteIDsPerTable(),
            4);
    }

    // Since DDL statements are in an auto-commit transaction, we can't use the query interface to
    // test the recovery algorithm and parallel read.
    void createTable(TableType tableType, TransactionTestType transactionTestType) {
        std::string tableName;
        switch (tableType) {
        case TableType::NODE: {
            tableName = "EXAM_PAPER";
            executeQueryWithoutCommit("CREATE NODE TABLE EXAM_PAPER(STUDENT_ID INT64, MARK DOUBLE, "
                                      "PRIMARY KEY(STUDENT_ID))");
        } break;
        case TableType::REL: {
            tableName = "likes";
            executeQueryWithoutCommit(
                "CREATE REL TABLE likes(FROM person TO organisation, RATING INT64, MANY_ONE)");
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
        ASSERT_FALSE(catalog->containsTable(&DUMMY_READ_TRANSACTION, tableName));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            ASSERT_FALSE(catalog->containsTable(&DUMMY_READ_TRANSACTION, tableName));
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_TRUE(catalog->containsTable(&DUMMY_READ_TRANSACTION, tableName));
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

    void dropTableCommitAndRecoveryTest(
        std::string tableName, TransactionTestType transactionTestType) {
        auto tableID = catalog->getTableID(&DUMMY_READ_TRANSACTION, tableName);
        auto tableSchema = catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID)->copy();
        executeQueryWithoutCommit(stringFormat("DROP TABLE {}", tableName));
        ASSERT_TRUE(catalog->containsTable(&DUMMY_READ_TRANSACTION, tableName));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            ASSERT_TRUE(catalog->containsTable(&DUMMY_READ_TRANSACTION, tableName));
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_FALSE(catalog->containsTable(&DUMMY_READ_TRANSACTION, tableName));
    }

    void dropNodeTableProperty(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit("ALTER TABLE person DROP gender");
        ASSERT_TRUE(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)
                        ->containProperty("gender"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            // The file for property gender should still exist until we do checkpoint.
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_FALSE(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)
                         ->containProperty("gender"));
        auto result = conn->query("MATCH (p:person) RETURN * ORDER BY p.ID LIMIT 1");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>{
                "{_ID: 0:0, _LABEL: person, ID: 0, fName: Alice, isStudent: True, isWorker: False, "
                "age: 35, eyeSight: 5.000000, birthdate: 1900-01-01, registerTime: 2011-08-20 "
                "11:25:30, lastJobDuration: 3 years 2 days 13:02:00, workedHours: [10,5], "
                "usedNames: [Aida], courseScoresPerTerm: [[10,8],[6,7,8]], grades: [96,54,86,92], "
                "height: 1.731000, u: a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"});
    }

    void dropRelTableProperty(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit("ALTER TABLE studyAt DROP places");
        ASSERT_TRUE(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, studyAtTableID)
                        ->containProperty("places"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            // The file for property places should still exist until we do checkpoint.
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_FALSE(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)
                         ->containProperty("places"));
        auto result = conn->query(
            "MATCH (:person)-[s:studyAt]->(:organisation) RETURN * ORDER BY s.year DESC LIMIT 1");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>{
                "(0:0)-{_LABEL: studyAt, _ID: 4:0, year: 2021, length: 5, level: 5, code: "
                "9223372036854775808, temprature: 32800, ulength: 33768, ulevel: 250, hugedata: "
                "1844674407370955161811111111}->(1:0)"});
    }

    void executeQueryWithoutCommit(std::string query) {
        auto preparedStatement = conn->prepare(query);
        auto mapper = PlanMapper(*getStorageManager(*database), getMemoryManager(*database),
            getCatalog(*database), conn->getClientContext());
        auto physicalPlan =
            mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[0].get(),
                preparedStatement->statementResult->getColumns());
        executionContext->clientContext->resetActiveQuery();
        getQueryProcessor(*database)->execute(physicalPlan.get(), executionContext.get());
    }

    void addPropertyToPersonTableWithoutDefaultValue(
        std::string propertyType, TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(stringFormat("ALTER TABLE person ADD random {}", propertyType));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        // The default value of the property is NULL if not specified by the user.
        auto result = conn->query("MATCH (p:person) return p.random");
        while (result->hasNext()) {
            ASSERT_TRUE(result->getNext()->getValue(0 /* idx */)->isNull());
        }
    }

    void addPropertyToPersonTableWithDefaultValue(std::string propertyType, std::string defaultVal,
        TransactionTestType transactionTestType, std::string expectedVal = "") {
        executeQueryWithoutCommit(
            "ALTER TABLE person ADD random " + propertyType + " DEFAULT " + defaultVal);
        // The convertResultToString function will remove the single quote around the result
        // std::string, so we should also remove the single quote in the expected result.
        defaultVal.erase(remove(defaultVal.begin(), defaultVal.end(), '\''), defaultVal.end());
        std::vector<std::string> expectedResult(
            8 /* numOfNodesInPesron */, expectedVal.empty() ? defaultVal : expectedVal);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_EQ(
            TestHelper::convertResultToString(*conn->query("MATCH (p:person) return p.random")),
            expectedResult);
    }

    void addPropertyToStudyAtTableWithoutDefaultValue(
        std::string propertyType, TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(stringFormat("ALTER TABLE studyAt ADD random {}", propertyType));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        // Note: the default value of the new property is NULL if not specified by the user.
        auto result = conn->query("MATCH (:person)-[e:studyAt]->(:organisation) return e.random");
        while (result->hasNext()) {
            ASSERT_TRUE(result->getNext()->getValue(0 /* idx */)->isNull());
        }
    }

    void addPropertyToStudyAtTableWithDefaultValue(
        std::string propertyType, std::string defaultVal, TransactionTestType transactionTestType) {
        executeQueryWithoutCommit(
            stringFormat("ALTER TABLE studyAt ADD random {} DEFAULT {}", propertyType, defaultVal));
        defaultVal.erase(remove(defaultVal.begin(), defaultVal.end(), '\''), defaultVal.end());
        std::vector<std::string> expectedResult(3 /* numOfRelsInStudyAt */, defaultVal);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_EQ(TestHelper::convertResultToString(
                      *conn->query("MATCH (:person)-[e:studyAt]->(:organisation) return e.random")),
            expectedResult);
    }

    void renameTable(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit("ALTER TABLE person RENAME TO student");
        ASSERT_EQ(catalog->getTableCatalogEntry(&DUMMY_WRITE_TRANSACTION, personTableID)->getName(),
            "student");
        ASSERT_EQ(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)->getName(),
            "person");
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            ASSERT_EQ(
                catalog->getTableCatalogEntry(&DUMMY_WRITE_TRANSACTION, personTableID)->getName(),
                "student");
            ASSERT_EQ(
                catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)->getName(),
                "person");
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_EQ(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)->getName(),
            "student");
        auto result = conn->query("MATCH (s:student) return s.age order by s.age");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>({"20", "20", "25", "30", "35", "40", "45", "83"}));
    }

    void renameProperty(TransactionTestType transactionTestType) {
        executeQueryWithoutCommit("ALTER TABLE person RENAME fName TO name");
        ASSERT_TRUE(catalog->getTableCatalogEntry(&DUMMY_WRITE_TRANSACTION, personTableID)
                        ->containProperty("name"));
        ASSERT_TRUE(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)
                        ->containProperty("fName"));
        if (transactionTestType == TransactionTestType::RECOVERY) {
            conn->query("COMMIT_SKIP_CHECKPOINT");
            ASSERT_TRUE(catalog->getTableCatalogEntry(&DUMMY_WRITE_TRANSACTION, personTableID)
                            ->containProperty("name"));
            ASSERT_TRUE(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)
                            ->containProperty("fName"));
            initWithoutLoadingGraph();
        } else {
            conn->query("COMMIT");
        }
        ASSERT_TRUE(catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, personTableID)
                        ->containProperty("name"));
        auto result = conn->query("MATCH (p:person) return p.name order by p.name");
        ASSERT_EQ(TestHelper::convertResultToString(*result),
            std::vector<std::string>({"Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg",
                "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"}));
    }

    Catalog* catalog;
    std::unique_ptr<BufferManager> bufferManager;
    std::unique_ptr<MemoryManager> memoryManager;
    std::unique_ptr<ExecutionContext> executionContext;
    std::unique_ptr<Profiler> profiler;
    table_id_t personTableID;
    table_id_t studyAtTableID;
};

TEST_F(TinySnbDDLTest, CreateNodeTableCommitRecovery) {
    createTable(TableType::NODE, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, CreateRelTableCommitRecovery) {
    createTable(TableType::REL, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DropNodeTableCommitRecovery) {
    dropTableCommitAndRecoveryTest("movies", TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DropRelTableCommitRecovery) {
    dropTableCommitAndRecoveryTest("knows", TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DropNodeTablePropertyRecovery) {
    dropNodeTableProperty(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, DropRelTablePropertyRecovery) {
    dropRelTableProperty(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "INT64" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddFixedListPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "DOUBLE[5]" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "STRING" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "INT64[]" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "STRING[]" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfStructPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "STRUCT(revenue int64, ages double[])[]" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddMapPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "MAP(STRING, INT64)" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddStructPropertyToPersonTableWithoutDefaultValueRecovery) {
    addPropertyToPersonTableWithoutDefaultValue(
        "STRUCT(price INT64[], volume INT64)" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue(
        "INT64" /* propertyType */, "21" /* defaultVal */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue("STRING" /* propertyType */,
        "'long long string'" /* defaultVal */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue("INT64[]" /* propertyType */,
        "[142,123,789]" /* defaultVal */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue("STRING[]" /* propertyType */,
        "['142','short','long long long string']" /* defaultValue */,
        TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfListOfStringPropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue("STRING[][]" /* propertyType */,
        "[['142','51'],['short','long','123'],['long long long string','short short short short',"
        "'short']]" /* defaultValue */,
        TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfStructPropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue(
        "STRUCT(revenue int64, ages double[])[]" /* propertyType */,
        "[{revenue: 144, ages: [3.200000,7.200000]}]" /* defaultValue */,
        TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddMapPropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue("MAP(STRING, INT64[])" /* propertyType */,
        "map(['key3'],[[3,2,1]])" /* defaultValue */, TransactionTestType::RECOVERY,
        "{key3=[3,2,1]}" /* expectedVal */);
}

TEST_F(TinySnbDDLTest, AddStructPropertyToPersonTableWithDefaultValueRecovery) {
    addPropertyToPersonTableWithDefaultValue(
        "STRUCT(price INT64[], volume INT64)" /* propertyType */,
        "{price: [5,3,2], volume: 24}" /* defaultValue */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToStudyAtTableWithoutDefaultValueRecovery) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "INT64" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToStudyAtTableWithoutDefaultValueRecovery) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "STRING" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfInt64PropertyToStudyAtTableWithoutDefaultValueRecovery) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "INT64[]" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToStudyAtTableWithoutDefaultValueRecovery) {
    addPropertyToStudyAtTableWithoutDefaultValue(
        "STRING[]" /* propertyType */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddInt64PropertyToStudyAtTableWithDefaultValueRecovery) {
    addPropertyToStudyAtTableWithDefaultValue(
        "INT64" /* propertyType */, "42" /* defaultVal */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddStringPropertyToStudyAtTableWithDefaultValueRecovery) {
    addPropertyToStudyAtTableWithDefaultValue("STRING" /* propertyType */,
        "'VERY SHORT STRING!!'" /* defaultVal */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfINT64PropertyToStudyAtTableWithDefaultValueRecovery) {
    addPropertyToStudyAtTableWithDefaultValue("INT64[]" /* propertyType */,
        "[5,6,7,1,3]" /* defaultVal */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfStringPropertyToStudyAtTableWithDefaultValueRecovery) {
    addPropertyToStudyAtTableWithDefaultValue("STRING[]" /* propertyType */,
        "['2','SHORT','SUPER LONG STRINGS']" /* defaultVal */, TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, AddListOfListOfStringPropertyToStudyAtTableWithDefaultValueRecovery) {
    addPropertyToStudyAtTableWithDefaultValue("STRING[][]" /* propertyType */,
        "[['hello','good','long long string test'],['6'],['very very long string']]" /*
        defaultVal*/
        ,
        TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, RenameTableRecovery) {
    renameTable(TransactionTestType::RECOVERY);
}

TEST_F(TinySnbDDLTest, RenamePropertyRecovery) {
    renameProperty(TransactionTestType::RECOVERY);
}

} // namespace testing
} // namespace kuzu
