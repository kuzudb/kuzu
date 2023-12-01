#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;

static void checkTuple(kuzu::processor::FlatTuple* tuple, const std::string& groundTruth) {
    ASSERT_STREQ(tuple->toString().c_str(), groundTruth.c_str());
}

TEST_F(ApiTest, MultiParamsPrepare) {
    auto preparedStatement = conn->prepare(
        "MATCH (a:person) WHERE a.fName STARTS WITH $n OR a.fName CONTAINS $xx RETURN COUNT(*)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("n"), "A"),
        std::make_pair(std::string("xx"), "ooq"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "2\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareBool) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), true));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "3\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareInt) {
    auto preparedStatement = conn->prepare("MATCH (a:person) WHERE a.age = 35 RETURN a.age + $1");
    auto result =
        conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), (int64_t)10));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "45\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareDouble) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.age = 35 RETURN a.eyeSight + $1");
    auto result =
        conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), (double_t)10.5));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "15.500000\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareString) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.fName STARTS WITH $n RETURN COUNT(*)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("n"), "A"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "1\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareDate) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.birthdate = $n RETURN COUNT(*)");
    auto result = conn->execute(
        preparedStatement.get(), std::make_pair(std::string("n"), Date::fromDate(1900, 1, 1)));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "2\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareTimestamp) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.registerTime = $n RETURN COUNT(*)");
    auto date = Date::fromDate(2011, 8, 20);
    auto time = Time::FromTime(11, 25, 30);
    auto result = conn->execute(preparedStatement.get(),
        std::make_pair(std::string("n"), Timestamp::fromDateTime(date, time)));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "1\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareInterval) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.lastJobDuration = $n RETURN COUNT(*)");
    std::string intervalStr = "3 years 2 days 13 hours 2 minutes";
    auto result = conn->execute(preparedStatement.get(),
        std::make_pair(
            std::string("n"), Interval::fromCString(intervalStr.c_str(), intervalStr.length())));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "2\n");
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareDefaultParam) {
    auto preparedStatement = conn->prepare("RETURN to_int8($1)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), "1"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "1\n");
    ASSERT_FALSE(result->hasNext());
    preparedStatement = conn->prepare("RETURN size($1)");
    result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), 1));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_STREQ(
        result->getErrorMessage().c_str(), "Parameter 1 has data type INT32 but expects STRING.");
}

TEST_F(ApiTest, PrepareDefaultListParam) {
    auto preparedStatement = conn->prepare("RETURN [1, $1]");
    auto result =
        conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), (int64_t)1));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "[1,1]\n");
    result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), "as"));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_STREQ(
        result->getErrorMessage().c_str(), "Parameter 1 has data type STRING but expects INT64.");
    preparedStatement = conn->prepare("RETURN [$1]");
    result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), "as"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "[as]\n");
    preparedStatement = conn->prepare("RETURN [to_int32($1)]");
    result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), "10"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "[10]\n");
}

TEST_F(ApiTest, PrepareDefaultStructParam) {
    auto preparedStatement = conn->prepare("RETURN {a:$1}");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), "10"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "{a: 10}\n");
    result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), 1));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_STREQ(
        result->getErrorMessage().c_str(), "Parameter 1 has data type INT32 but expects STRING.");
}

TEST_F(ApiTest, PrepareDefaultMapParam) {
    auto preparedStatement = conn->prepare("RETURN map([$1], [$2])");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), "10"),
        std::make_pair(std::string("2"), "abc"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "{10=abc}\n");
}

TEST_F(ApiTest, PrepareDefaultUnionParam) {
    auto preparedStatement = conn->prepare("RETURN union_value(a := $1)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), "10"));
    ASSERT_TRUE(result->hasNext());
    checkTuple(result->getNext().get(), "10\n");
}

TEST_F(ApiTest, PrepareLargeJoin) {
    auto preparedStatement = conn->prepare(
        " MATCH "
        "(:person)-[:knows]->(:person)-[:knows]->(:person)-[:knows]->(:person)-[:knows]->(:person)-"
        "[:knows]->(:person)-[:knows]->(:person)-[:knows]->(:person)-[:knows]->(:person)-[:knows]->"
        "(:person)-[:knows]->(:person)-[:knows]->(:person)-[:knows]->(:person)-[:knows]->(:person)-"
        "[:knows]->(:person)-[:knows]->(:person)-[:knows]->(:person) RETURN COUNT(*)");
    ASSERT_TRUE(preparedStatement->isSuccess());
}

TEST_F(ApiTest, ParamNotExist) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.fName STARTS WITH $n RETURN COUNT(*)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("a"), "A"));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_STREQ("Parameter a not found.", result->getErrorMessage().c_str());
}

TEST_F(ApiTest, ParamTypeError) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.fName STARTS WITH $n RETURN COUNT(*)");
    auto result =
        conn->execute(preparedStatement.get(), std::make_pair(std::string("n"), (int64_t)36));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_STREQ(
        "Parameter n has data type INT64 but expects STRING.", result->getErrorMessage().c_str());
}

TEST_F(ApiTest, MultipleExecutionOfPreparedStatement) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.fName STARTS WITH $n RETURN a.ID, a.fName");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("n"), "A"));
    auto groundTruth = std::vector<std::string>{"0|Alice"};
    ASSERT_EQ(groundTruth, TestHelper::convertResultToString(*result));
    result = conn->execute(preparedStatement.get(), std::make_pair(std::string("n"), "B"));
    groundTruth = std::vector<std::string>{"2|Bob"};
    ASSERT_EQ(groundTruth, TestHelper::convertResultToString(*result));
}
