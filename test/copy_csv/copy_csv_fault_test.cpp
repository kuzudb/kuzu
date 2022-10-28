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

class CopyCSVUTF8ByteMismatchTest : public CopyCSVFaultTest {
public:
    string getInputCSVDir() override { return "dataset/copy-csv-fault-tests/byte-mismatch/"; }
};

class CopyCSVUTF8InvalidUTFTest : public CopyCSVFaultTest {
public:
    string getInputCSVDir() override { return "dataset/copy-csv-fault-tests/invalid-utf8/"; }
};

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
    ASSERT_EQ(result->getErrorMessage(),
        "Binder exception: The name of column 1 does not match the column in schema. Expecting "
        "\"fName\", the column name in csv is \"name\".");

    result = conn->query(
        "COPY person FROM \"dataset/copy-csv-fault-tests/wrong-header/vPersonMissingColumn.csv\" "
        "(HEADER=true)");
    ASSERT_EQ(result->getErrorMessage(), "Binder exception: The csv file does not have sufficient "
                                         "property columns. Expecting at least 2 "
                                         "columns. The file has 1 property column.");

    result = conn->query(
        "COPY knows FROM \"dataset/copy-csv-fault-tests/wrong-header/eKnowsNoColumnMatch.csv\" "
        "(HEADER=true)");
    ASSERT_EQ(result->getErrorMessage(),
        "Binder exception: The first property column \"prop1\" is not found in the csv.");

    result = conn->query(
        "COPY knows FROM \"dataset/copy-csv-fault-tests/wrong-header/eKnowsMissingColumn.csv\" "
        "(HEADER=true)");
    ASSERT_EQ(result->getErrorMessage(),
        "Binder exception: The csv file does not have sufficient property columns. Expecting at "
        "least 2 columns. The file has 1 property column.");

    result = conn->query(
        "COPY knows FROM \"dataset/copy-csv-fault-tests/wrong-header/eKnowsWrongColumnName.csv\" "
        "(HEADER=true)");
    ASSERT_EQ(result->getErrorMessage(),
        "Binder exception: The name of column 1 does not match the column in schema. Expecting "
        "\"prop2\", the column name in csv is \"p2\".");
}

TEST_F(CopyCSVUTF8ByteMismatchTest, ByteMismatchTest) {
    ASSERT_EQ(
        "Failed to execute statement: COPY person FROM "
        "\"dataset/copy-csv-fault-tests/byte-mismatch/vPerson.csv\".\nError: CSVReader exception: "
        "Invalid UTF-8 character encountered.",
        getCopyCSVException());
}

TEST_F(CopyCSVUTF8InvalidUTFTest, InvalidUTFTest) {
    ASSERT_EQ(
        "Failed to execute statement: COPY person FROM "
        "\"dataset/copy-csv-fault-tests/invalid-utf8/vPerson.csv\".\nError: CSVReader exception: "
        "Invalid UTF-8 character encountered.",
        getCopyCSVException());
}
