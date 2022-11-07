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
