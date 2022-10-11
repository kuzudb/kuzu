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
