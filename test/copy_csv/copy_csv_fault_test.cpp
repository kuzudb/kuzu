#include "test/test_utility/include/test_helper.h"

#include "src/common/include/exception.h"

using namespace graphflow::testing;
using namespace std;

class CopyCSVFaultTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        databaseConfig->inMemoryMode = true;
        createDBAndConn();
    }

    string getCopyCSVException() {
        try {
            initGraph();
        } catch (Exception& e) { return e.what(); }
    }
};

class CopyCSVDuplicateIDTest : public CopyCSVFaultTest {
    string getInputCSVDir() override { return "dataset/copy-csv-fault-tests/duplicate-ids/"; }
};

class CopyNodeCSVUnmatchedColumnTypeTest : public CopyCSVFaultTest {
    string getInputCSVDir() override { return "dataset/copy-csv-fault-tests/long-string/"; }
};

class CopyCSVWrongHeaderTest : public CopyCSVFaultTest {};

TEST_F(CopyCSVDuplicateIDTest, DuplicateIDsError) {
    ASSERT_EQ(getCopyCSVException(),
        "Failed to execute statement: COPY person FROM "
        "\"dataset/copy-csv-fault-tests/duplicate-ids/vPerson.csv\".\nError: CopyCSV exception: ID "
        "value 10 violates the uniqueness constraint for the ID property.");
}

TEST_F(CopyNodeCSVUnmatchedColumnTypeTest, UnMatchedColumnTypeError) {
    conn->query(
        "create node table person (ID INT64, fName INT64, gender INT64, isStudent BOOLEAN, "
        "isWorker BOOLEAN, "
        "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
        "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], "
        "PRIMARY "
        "KEY (fName))");
    auto result = conn->query("COPY person FROM \"dataset/tinysnb/vPerson.csv\" (HEADER=true)");
    ASSERT_EQ(result->getErrorMessage(),
        "Cannot convert string Alice to INT64.. Invalid input. No characters consumed.");
}

TEST_F(CopyCSVWrongHeaderTest, CSVHeaderError) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    conn->query(
        "create rel table knows (FROM person TO person, prop1 INT64, prop2 STRING, MANY_MANY);");
    auto result = conn->query(
        "COPY person FROM \"dataset/copy-csv-fault-tests/wrong-header/vPersonWrongColumnName.csv\" "
        "(HEADER=true)");
    // We assert that CSV headers are ignored, so any mistakes there won't cause an error
    ASSERT_TRUE(result->isSuccess());
    result = conn->query(
        "COPY knows FROM \"dataset/copy-csv-fault-tests/wrong-header/eKnowsWrongColumnName.csv\" "
        "(HEADER=true)");
    ASSERT_TRUE(result->isSuccess());
}

TEST_F(CopyCSVWrongHeaderTest, CopyCSVToNonEmptyTableErrors) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    auto result = conn->query(
        "COPY person FROM \"dataset/copy-csv-fault-tests/wrong-header/vPersonWrongColumnName.csv\" "
        "(HEADER=true)");
    ASSERT_TRUE(result->isSuccess());
    result = conn->query(
        "COPY person FROM \"dataset/copy-csv-fault-tests/wrong-header/vPersonWrongColumnName.csv\" "
        "(HEADER=true)");
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(CopyCSVWrongHeaderTest, MissingColumnErrors) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    conn->query(
        "create rel table knows (FROM person TO person, prop1 INT64, prop2 STRING, MANY_MANY);");
    // We first copy nodes to the node table correctly, then check if missing columns will trigger
    // errors when copying rels.
    auto result = conn->query(
        "COPY person FROM \"dataset/copy-csv-fault-tests/wrong-header/vPerson.csv\" (HEADER=true)");
    ASSERT_TRUE(result->isSuccess());
    result = conn->query(
        "COPY knows FROM \"dataset/copy-csv-fault-tests/wrong-header/eKnowsMissingColumn.csv\""
        "(HEADER=true)");
    ASSERT_FALSE(result->isSuccess());
}
