#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class TinySnbCreateNodeTest : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        createDBAndConn();
    }
};

TEST_F(TinySnbCreateNodeTest, CreateNodeTableWithoutPK) {
    auto result = conn->query("CREATE NODE TABLE UNIVERSITY(NAME STRING, WEBSITE "
                              "STRING, REGISTER_TIME DATE)");
    ASSERT_EQ(result->isSuccess(), true);
    result = conn->query("MATCH (U:UNIVERSITY) RETURN U.NAME, U.WEBSITE");
    ASSERT_EQ(result->isSuccess(), true);
}

TEST_F(TinySnbCreateNodeTest, CreateNodeTableWithPK) {
    auto result = conn->query("CREATE NODE TABLE COMPANY(WEBSITE "
                              "STRING, RATING DOUBLE, ID INT64, PRIMARY KEY(WEBSITE))");
    ASSERT_EQ(result->isSuccess(), true);
    result = conn->query("MATCH (C:COMPANY) RETURN C.ID, C.WEBSITE");
    ASSERT_EQ(result->isSuccess(), true);
}
