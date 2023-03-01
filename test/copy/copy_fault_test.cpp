#include "common/exception.h"
#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::testing;

class CopyFaultTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
    }

    void validateCopyException(std::string copyQuery, std::string expectedException) {
        initGraph();
        auto result = conn->query(copyQuery);
        ASSERT_FALSE(result->isSuccess());
        ASSERT_STREQ(result->getErrorMessage().c_str(), expectedException.c_str());
    }
};

class CopyDuplicateIDTest : public CopyFaultTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/duplicate-ids/");
    }
};

class CopyNodeUnmatchedColumnTypeTest : public CopyFaultTest {};

class CopyWrongHeaderTest : public CopyFaultTest {};

class CopyRelTableMultiplicityViolationTest : public CopyFaultTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath(
            "dataset/copy-fault-tests/rel-table-multiplicity-violation/");
    }
};

class CopyInvalidNumberTest : public CopyFaultTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/invalid-number/");
    }
};

class CopyNullPKTest : public CopyFaultTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/null-pk/");
    }
};

TEST_F(CopyDuplicateIDTest, DuplicateIDsError) {
    validateCopyException(
        "COPY person FROM \"" +
            TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/duplicate-ids/vPerson.csv\""),
        "Copy exception: " + Exception::getExistedPKExceptionMsg("10"));
}

TEST_F(CopyNodeUnmatchedColumnTypeTest, UnMatchedColumnTypeError) {
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
    ASSERT_EQ(result->getErrorMessage(), "Invalid number: Alice.");
}

TEST_F(CopyWrongHeaderTest, HeaderError) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    conn->query(
        "create rel table knows (FROM person TO person, prop1 INT64, prop2 STRING, MANY_MANY);");
    auto result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-fault-tests/wrong-header/vPersonWrongColumnName.csv\" "
                        "(HEADER=true)"));
    // We assert that headers are ignored, so any mistakes there won't cause an error
    ASSERT_TRUE(result->isSuccess());
    result = conn->query("COPY knows FROM \"" +
                         TestHelper::appendKuzuRootPath(
                             "dataset/copy-fault-tests/wrong-header/eKnowsWrongColumnName.csv\" "
                             "(HEADER=true)"));
    ASSERT_TRUE(result->isSuccess());
}

TEST_F(CopyWrongHeaderTest, CopyToNonEmptyTableErrors) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    auto result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-fault-tests/wrong-header/vPersonWrongColumnName.csv\""
                        "(HEADER=true)"));
    ASSERT_TRUE(result->isSuccess());
    result = conn->query("COPY person FROM \"" +
                         TestHelper::appendKuzuRootPath(
                             "dataset/copy-fault-tests/wrong-header/vPersonWrongColumnName.csv\" "
                             "(HEADER=true)"));
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(CopyWrongHeaderTest, MissingColumnErrors) {
    conn->query("create node table person (ID INT64, fName STRING, PRIMARY KEY (ID));");
    conn->query(
        "create rel table knows (FROM person TO person, prop1 INT64, prop2 STRING, MANY_MANY);");
    // We first copy nodes to the node table correctly, then check if missing columns will trigger
    // errors when copying rels.
    auto result =
        conn->query("COPY person FROM \"" +
                    TestHelper::appendKuzuRootPath(
                        "dataset/copy-fault-tests/wrong-header/vPerson.csv\" (HEADER=true)"));
    ASSERT_TRUE(result->isSuccess());
    result = conn->query(
        "COPY knows FROM \"" + TestHelper::appendKuzuRootPath(
                                   "dataset/copy-fault-tests/wrong-header/eKnowsMissingColumn.csv\""
                                   "(HEADER=true)"));
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(CopyRelTableMultiplicityViolationTest, ManyOneMultiplicityViolationError) {
    validateCopyException(
        "COPY knows FROM \"" +
            TestHelper::appendKuzuRootPath(
                "dataset/copy-fault-tests/rel-table-multiplicity-violation/eKnows.csv\""),
        "Copy exception: RelTable knows is a MANY_ONE table, but node(nodeOffset: 0, tableName: "
        "person) has more than one neighbour in the forward direction.");
}

TEST_F(CopyRelTableMultiplicityViolationTest, OneManyMultiplicityViolationError) {
    validateCopyException(
        "COPY teaches FROM \"" +
            TestHelper::appendKuzuRootPath(
                "dataset/copy-fault-tests/rel-table-multiplicity-violation/eTeaches.csv\""),
        "Copy exception: RelTable teaches is a ONE_MANY table, but node(nodeOffset: 2, "
        "tableName: person) has more than one neighbour in the backward direction.");
}

TEST_F(CopyRelTableMultiplicityViolationTest, OneOneMultiplicityViolationError) {
    validateCopyException(
        "COPY matches FROM \"" +
            TestHelper::appendKuzuRootPath(
                "dataset/copy-fault-tests/rel-table-multiplicity-violation/eMatches.csv\""),
        "Copy exception: RelTable matches is a ONE_ONE table, but node(nodeOffset: 1, "
        "tableName: person) has more than one neighbour in the forward direction.");
}

TEST_F(CopyInvalidNumberTest, INT32OverflowError) {
    validateCopyException(
        "COPY person FROM \"" +
            TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/invalid-number/vPerson.csv\""),
        "Invalid number: 2147483650.");
}

TEST_F(CopyInvalidNumberTest, InvalidNumberError) {
    validateCopyException(
        "COPY person FROM \"" +
            TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/invalid-number/vMovie.csv\""),
        "Invalid number: 312abc.");
}

TEST_F(CopyNullPKTest, NullPKErrpr) {
    validateCopyException(
        "COPY person FROM \"" +
            TestHelper::appendKuzuRootPath("dataset/copy-fault-tests/null-pk/vPerson.csv\""),
        "Reader exception: Primary key cannot be null.");
}
