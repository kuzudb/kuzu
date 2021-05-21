#include "gtest/gtest.h"
#include "test/binder/binder_test_utils.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/query_binder.h"
#include "src/parser/include/parser.h"

using ::testing::NiceMock;
using ::testing::Test;

class BinderTest : public Test {

public:
    static unique_ptr<BoundSingleQuery> getBoundQuery(const string& input, const Catalog& catalog) {
        auto parsedQuery = Parser::parseQuery(input);
        return QueryBinder(catalog).bind(*parsedQuery);
    }
};

TEST_F(BinderTest, LOADCSVBasicTest) {
    auto expectedCSVColumnInfo = vector<pair<string, DataType>>();
    expectedCSVColumnInfo.emplace_back(make_pair("id", NODE));
    expectedCSVColumnInfo.emplace_back(make_pair("name", STRING));
    expectedCSVColumnInfo.emplace_back(make_pair("orgCode", INT32));
    expectedCSVColumnInfo.emplace_back(make_pair("mark", DOUBLE));
    auto expectedLoadCSVStatement = make_unique<BoundLoadCSVStatement>(
        "dataset/tinysnb/vOrganisation.csv", ',', "csvLine", move(expectedCSVColumnInfo));

    NiceMock<PersonKnowsPersonCatalog> catalog;
    catalog.setUp();

    auto query = "LOAD CSV WITH HEADERS FROM \"dataset/tinysnb/vOrganisation.csv\" AS csvLine "
                 "RETURN COUNT(*);";
    auto boundQuery = BinderTest::getBoundQuery(query, catalog);
    ASSERT_TRUE(BinderTestUtils::equals(
        *expectedLoadCSVStatement, (BoundLoadCSVStatement&)*boundQuery->boundReadingStatements[0]));
}

TEST_F(BinderTest, LOADCSVExceptionTest) {
    NiceMock<PersonKnowsPersonCatalog> catalog;
    catalog.setUp();

    auto query = "LOAD CSV WITH HEADERS FROM \"dummy\" AS csvLine "
                 "RETURN COUNT(*);";
    try {
        BinderTest::getBoundQuery(query, catalog);
    } catch (const invalid_argument& exception) {
        ASSERT_STREQ("Cannot open file at dummy.", exception.what());
    }
}
