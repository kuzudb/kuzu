#include "common/exception.h"
#include "test_helper/test_helper.h"

using namespace kuzu::testing;
using namespace std;

class CopyCSVFaultTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        databaseConfig->inMemoryMode = true;
        createDBAndConn();
    }

    void validateCopyCSVException(string copyCSVQuery, string expectedException) {
        initGraph();
        auto result = conn->query(copyCSVQuery);
        ASSERT_FALSE(result->isSuccess());
        ASSERT_STREQ(result->getErrorMessage().c_str(), expectedException.c_str());
    }
};

class CopyCSVDuplicateIDTest : public CopyCSVFaultTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-csv-fault-tests/duplicate-ids/");
    }
};

class CopyNodeCSVUnmatchedColumnTypeTest : public CopyCSVFaultTest {};

class CopyCSVWrongHeaderTest : public CopyCSVFaultTest {};

class CopyCSVRelTableMultiplicityViolationTest : public CopyCSVFaultTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath(
            "dataset/copy-csv-fault-tests/rel-table-multiplicity-violation/");
    }
};

TEST_F(CopyCSVDuplicateIDTest, DuplicateIDsError) {
    validateCopyCSVException(
        "COPY person FROM \"" + TestHelper::appendKuzuRootPath(
                                    "dataset/copy-csv-fault-tests/duplicate-ids/vPerson.csv\""),
        "CopyCSV exception: " + Exception::getExistedPKExceptionMsg("10"));
}

TEST_F(CopyNodeCSVUnmatchedColumnTypeTest, UnMatchedColumnTypeError) {
    conn->query(
        "create node table person (ID INT64, fName INT64, gender INT64, isStudent BOOLEAN, "
        "isWorker BOOLEAN, "
        "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
        "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], "
        "PRIMARY "
        "KEY (fName))");
    auto result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath("dataset/tinysnb/vPerson.csv\" (HEADER=true)"));
    ASSERT_EQ(result->getErrorMessage(),
        "Cannot convert string Alice to INT64.. Invalid input. No characters consumed.");
}

TEST_F(CopyCSVWrongHeaderTest, CSVHeaderError) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    conn->query(
        "create rel table knows (FROM person TO person, prop1 INT64, prop2 STRING, MANY_MANY);");
    auto result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-csv-fault-tests/wrong-header/vPersonWrongColumnName.csv\" "
                        "(HEADER=true)"));
    // We assert that CSV headers are ignored, so any mistakes there won't cause an error
    ASSERT_TRUE(result->isSuccess());
    result =
        conn->query("COPY knows FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-csv-fault-tests/wrong-header/eKnowsWrongColumnName.csv\" "
                        "(HEADER=true)"));
    ASSERT_TRUE(result->isSuccess());
}

TEST_F(CopyCSVWrongHeaderTest, CopyCSVToNonEmptyTableErrors) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    auto result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-csv-fault-tests/wrong-header/vPersonWrongColumnName.csv\""
                        "(HEADER=true)"));
    ASSERT_TRUE(result->isSuccess());
    result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-csv-fault-tests/wrong-header/vPersonWrongColumnName.csv\" "
                        "(HEADER=true)"));
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(CopyCSVWrongHeaderTest, MissingColumnErrors) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    conn->query(
        "create rel table knows (FROM person TO person, prop1 INT64, prop2 STRING, MANY_MANY);");
    // We first copy nodes to the node table correctly, then check if missing columns will trigger
    // errors when copying rels.
    auto result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-csv-fault-tests/wrong-header/vPerson.csv\" (HEADER=true)"));
    ASSERT_TRUE(result->isSuccess());
    result = conn->query("COPY knows FROM \"" +
                         TestHelper::appendKuzuRootPath(
                             "dataset/copy-csv-fault-tests/wrong-header/eKnowsMissingColumn.csv\""
                             "(HEADER=true)"));
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(CopyCSVRelTableMultiplicityViolationTest, ManyOneMultiplicityViolationError) {
    validateCopyCSVException(
        "COPY knows FROM \"" +
            TestHelper::appendKuzuRootPath(
                "dataset/copy-csv-fault-tests/rel-table-multiplicity-violation/eKnows.csv\""),
        "CopyCSV exception: RelTable knows is a MANY_ONE table, but node(nodeOffset: 0, tableName: "
        "person) has more than one neighbour in the forward direction.");
}

TEST_F(CopyCSVRelTableMultiplicityViolationTest, OneManyMultiplicityViolationError) {
    validateCopyCSVException(
        "COPY teaches FROM \"" +
            TestHelper::appendKuzuRootPath(
                "dataset/copy-csv-fault-tests/rel-table-multiplicity-violation/eTeaches.csv\""),
        "CopyCSV exception: RelTable teaches is a ONE_MANY table, but node(nodeOffset: 2, "
        "tableName: person) has more than one neighbour in the backward direction.");
}

TEST_F(CopyCSVRelTableMultiplicityViolationTest, OneOneMultiplicityViolationError) {
    validateCopyCSVException(
        "COPY matches FROM \"" +
            TestHelper::appendKuzuRootPath(
                "dataset/copy-csv-fault-tests/rel-table-multiplicity-violation/eMatches.csv\""),
        "CopyCSV exception: RelTable matches is a ONE_ONE table, but node(nodeOffset: 1, "
        "tableName: person) has more than one neighbour in the forward direction.");
}
