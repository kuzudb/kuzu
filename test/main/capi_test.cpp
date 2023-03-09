#include "graph_test/graph_test.h"

using namespace kuzu::testing;

class CApiTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        const char* dbPath = databasePath.c_str();
        database = std::make_unique<Database>(dbPath, *systemConfig);
        conn = std::make_unique<Connection>(database.get());
        initGraph();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiTest, Basic) {
    char query[] = "MATCH (a:person) RETURN COUNT(*)";
    auto result = conn->kuzu_query(query);
    auto groundTruth = std::vector<std::string>{"8"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}
