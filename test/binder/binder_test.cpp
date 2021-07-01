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
    expectedCSVColumnInfo.emplace_back(make_pair("age", INT32));
    expectedCSVColumnInfo.emplace_back(make_pair("name", STRING));
    auto csvLines = vector<shared_ptr<Expression>>();
    auto csvLine0 = make_shared<Expression>(CSV_LINE_EXTRACT, INT32, "_gf0_csvLine[0]");
    csvLine0->alias = "csvLine[0]";
    auto csvLine1 = make_shared<Expression>(CSV_LINE_EXTRACT, STRING, "_gf0_csvLine[1]");
    csvLine1->alias = "csvLine[1]";
    csvLines.push_back(csvLine0);
    csvLines.push_back(csvLine1);
    auto expectedLoadCSVStatement =
        make_unique<BoundLoadCSVStatement>("dataset/tinysnb/params.csv", ',', move(csvLines));

    NiceMock<TinySnbCatalog> catalog;
    catalog.setUp();

    auto query = "LOAD CSV WITH HEADERS FROM \"dataset/tinysnb/params.csv\" AS csvLine "
                 "RETURN COUNT(*);";
    auto boundQuery = BinderTest::getBoundQuery(query, catalog);
    ASSERT_TRUE(BinderTestUtils::equals(
        *expectedLoadCSVStatement, (BoundLoadCSVStatement&)*boundQuery->boundReadingStatements[0]));
}

TEST_F(BinderTest, LOADCSVMATCHTest) {
    auto a = make_shared<NodeExpression>("_gf1_a", 0);
    a->alias = "a";
    auto aName = make_shared<PropertyExpression>("_gf1_a.name", STRING, 1, move(a));
    auto csvLine1 = make_shared<Expression>(CSV_LINE_EXTRACT, STRING, "_gf0_csvLine[1]");
    csvLine1->alias = "csvLine[1]";
    auto expectedWhere = make_shared<Expression>(EQUALS, BOOL, move(aName), move(csvLine1));

    NiceMock<TinySnbCatalog> catalog;
    catalog.setUp();

    auto query = "LOAD CSV WITH HEADERS FROM \"dataset/tinysnb/params.csv\" AS csvLine "
                 "MATCH (a:person) where a.name = csvLine[1] RETURN COUNT(*);";
    auto boundQuery = BinderTest::getBoundQuery(query, catalog);
    ASSERT_TRUE(BinderTestUtils::equals(*expectedWhere,
        *((BoundMatchStatement&)*boundQuery->boundReadingStatements[1]).whereExpression));
}

TEST_F(BinderTest, LOADCSVExceptionTest) {
    NiceMock<TinySnbCatalog> catalog;
    catalog.setUp();

    auto query = "LOAD CSV WITH HEADERS FROM \"dummy\" AS csvLine "
                 "RETURN COUNT(*);";
    try {
        BinderTest::getBoundQuery(query, catalog);
    } catch (const invalid_argument& exception) {
        ASSERT_STREQ("Cannot open file at dummy.", exception.what());
    }
}

TEST_F(BinderTest, DateNodePropertyBindingTest) {
    NiceMock<TinySnbCatalog> catalog;
    catalog.setUp();

    auto query = "MATCH (a:person)-[e:knows]->(b:person) where a.birthdate < b.birthdate "
                 "AND e.knowsdate < b.birthdate RETURN COUNT(*);";
    auto boundQuery = BinderTest::getBoundQuery(query, catalog);
    BoundMatchStatement* matchStatement =
        (BoundMatchStatement*)(boundQuery->boundReadingStatements[0].get());
    auto& leftANDExpr = matchStatement->whereExpression->childrenExpr[0];
    auto& rightANDExpr = matchStatement->whereExpression->childrenExpr[1];
    ASSERT_EQ(DATE, leftANDExpr->childrenExpr[0]->dataType);
    ASSERT_EQ(DATE, leftANDExpr->childrenExpr[1]->dataType);
    ASSERT_EQ(DATE, rightANDExpr->childrenExpr[0]->dataType);
    ASSERT_EQ(DATE, rightANDExpr->childrenExpr[1]->dataType);
}
