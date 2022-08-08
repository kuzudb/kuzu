#include "test/test_utility/include/test_helper.h"

#include "src/common/include/exception.h"

using namespace graphflow::testing;
using namespace std;

class CopyCSVLongStringTest : public InMemoryDBTest {
    void initGraph() override {}
};

class CopyCSVDuplicateIDTest : public InMemoryDBTest {
    void initGraph() override {}
};

class CopyNodeCSVUnmatchedColumnTypeTest : public InMemoryDBTest {
    void initGraph() override {}
};

TEST_F(CopyCSVLongStringTest, LongStringError) {
    conn->query(
        createNodeCmdPrefix + "person (ID INT64, fName STRING, gender INT64, PRIMARY KEY (ID))");
    auto result =
        conn->query("COPY person FROM \"dataset/copy-csv-fault-tests/long-string/vPerson.csv\"");
    ASSERT_EQ(result->getErrorMessage(), "CSVReader exception: Maximum length of strings is "
                                         "4096. Input string's length is 5625.");
}

TEST_F(CopyCSVDuplicateIDTest, DuplicateIDsError) {
    conn->query(createNodeCmdPrefix + "person (ID INT64, fName STRING, PRIMARY KEY (ID))");
    auto result =
        conn->query("COPY person FROM \"dataset/copy-csv-fault-tests/duplicate-ids/vPerson.csv\"");
    ASSERT_EQ(result->getErrorMessage(), "CopyCSV exception: ID value 10 violates the uniqueness "
                                         "constraint for the ID property.");
}

TEST_F(CopyNodeCSVUnmatchedColumnTypeTest, UnMatchedColumnTypeError) {
    conn->query(
        createNodeCmdPrefix +
        "person (ID INT64, fName INT64, gender INT64, isStudent BOOLEAN, isWorker BOOLEAN, "
        "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
        "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], "
        "PRIMARY "
        "KEY (fName))");
    auto result = conn->query("COPY person FROM \"dataset/tinysnb/vPerson.csv\"");
    ASSERT_EQ(result->getErrorMessage(),
        "Cannot convert string Alice to INT64.. Invalid input. No characters consumed.");
}
