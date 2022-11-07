#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class TinySnbExceptionTest : public DBTest {
public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

TEST_F(TinySnbExceptionTest, ReadVarlengthRelPropertyTest1) {
    auto result = conn->query("MATCH (a:person)-[e:knows*1..3]->(b:person) RETURN e;");
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
