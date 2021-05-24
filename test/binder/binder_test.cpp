#include "gtest/gtest.h"
#include "test/binder/binder_test_utils.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/bound_statements/bound_match_statement.h"
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
    auto csvLine = make_shared<Expression>(VARIABLE, LIST_STRING, "_gf0_csvLine");
    csvLine->alias = "csvLine";
    auto expectedLoadCSVStatement = make_unique<BoundLoadCSVStatement>(
        "dataset/tinysnb/vOrganisation.csv", ',', move(expectedCSVColumnInfo), csvLine);

    NiceMock<PersonKnowsPersonCatalog> catalog;
    catalog.setUp();

    auto query = "LOAD CSV WITH HEADERS FROM \"dataset/tinysnb/vOrganisation.csv\" AS csvLine "
                 "RETURN COUNT(*);";
    auto boundQuery = BinderTest::getBoundQuery(query, catalog);
    ASSERT_TRUE(BinderTestUtils::equals(
        *expectedLoadCSVStatement, (BoundLoadCSVStatement&)*boundQuery->boundReadingStatements[0]));
}

TEST_F(BinderTest, LOADCSVMATCHTest) {
    auto a = make_shared<NodeExpression>("_gf1_a", 0);
    a->alias = "a";
    auto aName = make_shared<PropertyExpression>("_gf1_a.name", STRING, 1, move(a));
    auto csvLine = make_shared<Expression>(VARIABLE, LIST_STRING, "_gf0_csvLine");
    csvLine->alias = "csvLine";
    auto zero = make_shared<LiteralExpression>(LITERAL_INT, INT32, Value((int32_t)0u));
    auto csvLine0 = make_shared<Expression>(LIST_EXTRACT, STRING, move(csvLine), move(zero));
    csvLine0->variableName = "_gf0_csvLine[0]";
    auto expectedWhere = make_shared<Expression>(EQUALS, BOOL, move(aName), move(csvLine0));

    NiceMock<PersonKnowsPersonCatalog> catalog;
    catalog.setUp();

    auto query = "LOAD CSV WITH HEADERS FROM \"dataset/tinysnb/vOrganisation.csv\" AS csvLine "
                 "MATCH (a:person) where a.name = csvLine[0] RETURN COUNT(*);";
    auto boundQuery = BinderTest::getBoundQuery(query, catalog);
    ASSERT_TRUE(BinderTestUtils::equals(*expectedWhere,
        *((BoundMatchStatement&)*boundQuery->boundReadingStatements[1]).whereExpression));
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
