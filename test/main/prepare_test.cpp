#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;

TEST_F(ApiTest, MultiParamsPrepare) {
    auto preparedStatement = conn->prepare(
        "MATCH (a:person) WHERE a.fName STARTS WITH $n OR a.fName CONTAINS $xx RETURN COUNT(*)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("n"), "A"),
        std::make_pair(std::string("xx"), "ooq"));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 2);
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareBool) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), true));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 3);
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareInt) {
    auto preparedStatement = conn->prepare("MATCH (a:person) WHERE a.age = 35 RETURN a.age + $1");
    auto result =
        conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), (int64_t)10));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 45);
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareDouble) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.age = 35 RETURN a.eyeSight + $1");
    auto result =
        conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), (double_t)10.5));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<double>(), 15.5);
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareString) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.fName STARTS WITH $n RETURN COUNT(*)");
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("n"), "A"));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 1);
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareDate) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.birthdate = $n RETURN COUNT(*)");
    auto result = conn->execute(
        preparedStatement.get(), std::make_pair(std::string("n"), Date::FromDate(1900, 1, 1)));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 2);
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareTimestamp) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.registerTime = $n RETURN COUNT(*)");
    auto date = Date::FromDate(2011, 8, 20);
    auto time = Time::FromTime(11, 25, 30);
    auto result = conn->execute(preparedStatement.get(),
        std::make_pair(std::string("n"), Timestamp::FromDatetime(date, time)));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 1);
    ASSERT_FALSE(result->hasNext());
}

TEST_F(ApiTest, PrepareInterval) {
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.lastJobDuration = $n RETURN COUNT(*)");
    std::string intervalStr = "3 years 2 days 13 hours 2 minutes";
    auto result = conn->execute(preparedStatement.get(),
        std::make_pair(
            std::string("n"), Interval::FromCString(intervalStr.c_str(), intervalStr.length())));
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 2);
    ASSERT_FALSE(result->hasNext());
}

// TEST_F(ApiTest, DefaultParam) {
//    auto preparedStatement = conn->prepare("MATCH (a:person) WHERE $1 = $2 RETURN COUNT(*)");
//    auto result =
//        conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), (int64_t)1.4),
//            std::make_pair(std::string("2"), (int64_t)1.4));
//    ASSERT_TRUE(result->hasNext());
//    auto tuple = result->getNext();
//    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 8);
//    ASSERT_FALSE(result->hasNext());
//}

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
