#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::common;
using namespace kuzu::testing;

class TinySnbExceptionTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(TinySnbExceptionTest, ReadVarlengthRelPropertyTest1) {
    auto result = conn->query("MATCH (a:person)-[e:knows*1..3]->(b:person) RETURN e.age;");
    ASSERT_STREQ("Binder exception: Cannot read property of variable length rel e.",
        result->getErrorMessage().c_str());
}

TEST_F(TinySnbExceptionTest, ReadVarlengthRelPropertyTest2) {
    auto result =
        conn->query("MATCH (a:person)-[e:knows*1..3]->(b:person) WHERE ID(e) = 0 RETURN COUNT(*);");
    ASSERT_STREQ("Binder exception: Cannot read property of variable length rel e.",
        result->getErrorMessage().c_str());
}

TEST_F(TinySnbExceptionTest, AccessRelInternalIDTest) {
    auto result =
        conn->query("MATCH (a:person)-[e:knows]->(b:person) WHERE e._id > 1 RETURN COUNT(*);");
    ASSERT_STREQ(
        "Binder exception: _id is reserved for system usage. External access is not allowed.",
        result->getErrorMessage().c_str());
}

TEST_F(TinySnbExceptionTest, InsertNodeWithoutPrimaryKeyTest) {
    auto result = conn->query("CREATE (a:person {isWorker:true});");
    ASSERT_STREQ("Binder exception: Create node a expects primary key ID as input.",
        result->getErrorMessage().c_str());
}

TEST_F(TinySnbExceptionTest, InsertNodeWithExistedPKError1) {
    auto result = conn->query("CREATE (a:person {ID:0, fName:'Charlie'});");
    auto expectedErrorMsg = "Runtime exception: " + Exception::getExistedPKExceptionMsg("0");
    ASSERT_STREQ(expectedErrorMsg.c_str(), result->getErrorMessage().c_str());
}

TEST_F(TinySnbExceptionTest, InsertNodeWithExistedPKError2) {
    auto result = conn->query("CREATE (a:person {ID:100, fName:'Charlie'});");
    ASSERT_TRUE(result->isSuccess());
    result = conn->query("CREATE (a:person {ID:100, fName:'Guodong'});");
    auto expectedErrorMsg = "Runtime exception: " + Exception::getExistedPKExceptionMsg("100");
    ASSERT_STREQ(expectedErrorMsg.c_str(), result->getErrorMessage().c_str());
}

TEST_F(TinySnbExceptionTest, InsertNodeWithNullPK) {
    auto result = conn->query("CREATE (a:person {ID:NULL, fName:'Guodong'});");
    std::string expectedErrorMsg = "Runtime exception: Null is not allowed as a primary key value.";
    ASSERT_STREQ(expectedErrorMsg.c_str(), result->getErrorMessage().c_str());
}

TEST_F(TinySnbExceptionTest, DeleteNodeWithEdgeErrorTest) {
    auto result = conn->query("MATCH (a:person) WHERE a.ID = 0 DELETE a");
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(TinySnbExceptionTest, CastStrToTimestampError) {
    // Missing day.
    auto result = conn->query("MATCH (a:person) return timestamp(\"2112-08 08:21:23.005612\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing timestamp. Given: \"2112-08 08:21:23.005612\". Expected "
        "format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])");

    // Missing second.
    result = conn->query("MATCH (a:person) return timestamp(\"2112-08-04 08:23.005612\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing timestamp. Given: \"2112-08-04 08:23.005612\". Expected "
        "format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])");

    // Missing a digit in day.
    result = conn->query("MATCH (a:person) return timestamp(\"2112-08-0\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(), "Date out of range: 2112-8-0.");

    // Invalid timezone format.
    result = conn->query("MATCH (a:person) return timestamp(\"1992-04-28T09:33:56-XX:DD\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing timestamp. Given: \"1992-04-28T09:33:56-XX:DD\". Expected "
        "format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])");

    // Missing +/- sign.
    result = conn->query("MATCH (a:person) return timestamp(\"2112-08-21 08:21:23.005612Z02:00\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing timestamp. Given: \"2112-08-21 08:21:23.005612Z02:00\". "
        "Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])");

    // Incorrect timezone minutes.
    result =
        conn->query("MATCH (a:person) return timestamp(\"2112-08-21 08:21:23.005612Z+02:100\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing timestamp. Given: \"2112-08-21 08:21:23.005612Z+02:100\". "
        "Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])");

    // Incorrect timezone hours.
    result = conn->query("MATCH (a:person) return timestamp(\"2112-08-21 08:21:23.005612Z+021\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing timestamp. Given: \"2112-08-21 08:21:23.005612Z+021\". "
        "Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])");
}

TEST_F(TinySnbExceptionTest, CastStrToIntervalError) {
    // Missing the specifier string (eg. days, minutes...).
    auto result = conn->query("MATCH (a:person) return interval(\"3 years 2 months 45\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing interval. Field name is missing.");

    // Missing the number before the specifier string.
    result =
        conn->query("MATCH (a:person) return interval(\"20 years 30 months 20 days minutes\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing interval. Given: \"20 years 30 months 20 days minutes\".");

    // Numbers and specifier string are not separated by spaces.
    result = conn->query("MATCH (a:person) return interval(\"2 years 30 minutes20 seconds\")");
    ASSERT_STREQ(
        result->getErrorMessage().c_str(), "Unrecognized interval specifier string: minutes20.");

    // Unrecognized specifier string (millseconds).
    result = conn->query(
        "MATCH (a:person) return interval(\"10 years 2 days 48 hours 28 seconds 12 millseconds\")");
    ASSERT_STREQ(
        result->getErrorMessage().c_str(), "Unrecognized interval specifier string: millseconds.");

    // Multiple specifier strings.
    result = conn->query(
        "MATCH (a:person) return interval(\"10 years 2 days 48 hours 28 seconds microseconds\")");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Error occurred during parsing interval. Given: \"10 "
        "years 2 days 48 hours 28 seconds microseconds\".");
}

TEST_F(TinySnbExceptionTest, SetListPropertyError) {
    auto result = conn->query("MATCH (a:person) SET a.workedHours=['A', 'B']");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Expression LIST_CREATION(A,B) has data type STRING[] but expect "
        "INT64[]. "
        "Implicit cast is not supported.");
}

TEST_F(TinySnbExceptionTest, ListFunctionMatchError1) {
    auto result = conn->query("MATCH (a:person) RETURN array_concat(a.workedHours, a.age)");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Cannot match a built-in function for given function "
        "ARRAY_CONCAT(VAR_LIST,INT64). Supported inputs are\n(VAR_LIST,VAR_LIST) -> VAR_LIST\n");
}

TEST_F(TinySnbExceptionTest, ListFunctionMatchError2) {
    auto result = conn->query("MATCH (a:person) RETURN array_concat(a.workedHours, ['A'])");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Cannot bind LIST_CONCAT with parameter type INT64[] and STRING[].");
}

TEST_F(TinySnbExceptionTest, ListFunctionMatchError3) {
    auto result = conn->query("MATCH (a:person) RETURN [a.age, a.fName]");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Cannot bind LIST_CREATION with parameter type INT64 and STRING.");
}

TEST_F(TinySnbExceptionTest, ListPrepareError) {
    auto preparedStatement = conn->prepare("MATCH (a:person) RETURN len($1)");
    ASSERT_STREQ(preparedStatement->getErrorMessage().c_str(),
        "Binder exception: Cannot resolve recursive data type for expression $1.");
}

TEST_F(TinySnbExceptionTest, ParsingErrorRollbackTest) {
    conn->beginWriteTransaction();
    conn->query("create (p:person {ID: 100})");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (:person) RETURN count(*)")),
        std::vector<std::string>{"9"});
    auto result = conn->query("RETURN make_date(2011,1,32)");
    ASSERT_STREQ(result->getErrorMessage().c_str(), "Date out of range: 2011-1-32.");
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query("MATCH (:person) RETURN count(*)")),
        std::vector<std::string>{"8"});
}

TEST_F(TinySnbExceptionTest, DivideBy0Error) {
    auto result = conn->query("RETURN 1 / 0");
    ASSERT_STREQ(result->getErrorMessage().c_str(), "Runtime exception: Divide by zero.");
}

TEST_F(TinySnbExceptionTest, ModuloBy0Error) {
    auto result = conn->query("RETURN 1 % 0");
    ASSERT_STREQ(result->getErrorMessage().c_str(), "Runtime exception: Modulo by zero.");
}

TEST_F(TinySnbExceptionTest, EmptyQuery) {
    auto result = conn->query("");
    ASSERT_STREQ(result->getErrorMessage().c_str(), "Connection Exception: Query is empty.");
}

TEST_F(TinySnbExceptionTest, ReadAfterUpdate2) {
    auto result = conn->query(
        "MATCH (a:person) WHERE a.age = 35 DELETE a WITH a MATCH (a)-[:knows]->(b:person) "
        "RETURN a.age");
    ASSERT_STREQ(
        result->getErrorMessage().c_str(), "Binder exception: Read after update is not supported.");
}

TEST_F(TinySnbExceptionTest, Overflow) {
    auto result = conn->query("RETURN to_int16(10000000000)");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Runtime exception: Cast failed. 10000000000 is not in INT16 range.");
}

TEST_F(TinySnbExceptionTest, Int32PrimaryKey) {
    auto result = conn->query("CREATE NODE TABLE play(a INT32, PRIMARY KEY (a))");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Invalid primary key type: INT32. Expected STRING or INT64.");
}

TEST_F(TinySnbExceptionTest, MultiLabelUpdate) {
    std::unique_ptr<QueryResult> result;
    result = conn->query("CREATE (a:person:organisation {ID:0})");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Create node a with multiple node labels is not supported.");
    result = conn->query("MATCH (a:person), (b:person:organisation) CREATE (a)-[e:knows]->(b)");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Create rel e with multiple rel labels or bound by multiple node labels "
        "is not supported.");
    result = conn->query("MATCH (a:person:organisation) DELETE a");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Delete node a with multiple node labels is not supported.");
    result = conn->query("MATCH (a:person:organisation)-[e:knows]->(b:person) DELETE e");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Delete rel e with multiple rel labels or bound by multiple node labels "
        "is not supported.");
    result = conn->query("MATCH (a:person:organisation) SET a.age = 0");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Set property of node a with multiple node labels is not supported.");
    result = conn->query(
        "MATCH (a:person:organisation)-[e:knows]->(b:person) SET e.date=date('2022-12-12')");
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Binder exception: Set property of rel e with multiple rel labels or bound by multiple "
        "node labels is not supported.");
}
