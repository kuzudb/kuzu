#include "gtest/gtest.h"
#include "test/binder/binder_test_utils.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/binder/include/query_binder.h"
#include "src/parser/include/parser.h"

using ::testing::NiceMock;
using ::testing::Test;

/**
 * Remove Load csv related test once LIST data type is supported.
 * Remove subquery related test once end-to-end subquery test is supported.
 */
class BinderTest : public Test {

public:
    static unique_ptr<BoundSingleQuery> getBoundQuery(const string& input, const Catalog& catalog) {
        auto parsedQuery = Parser::parseQuery(input);
        return QueryBinder(catalog).bind(*parsedQuery);
    }
};

TEST_F(BinderTest, LOADCSVBasicTest) {
    auto expectedCSVColumnInfo = vector<pair<string, DataType>>();
    expectedCSVColumnInfo.emplace_back(make_pair("age", INT64));
    expectedCSVColumnInfo.emplace_back(make_pair("name", STRING));
    auto csvLines = vector<shared_ptr<VariableExpression>>();
    auto csvLine0 = make_shared<VariableExpression>(CSV_LINE_EXTRACT, INT64, "_0_csvLine[0]");
    csvLine0->rawExpression = "csvLine[0]";
    auto csvLine1 = make_shared<VariableExpression>(CSV_LINE_EXTRACT, STRING, "_1_csvLine[1]");
    csvLine1->rawExpression = "csvLine[1]";
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
    auto a = make_shared<NodeExpression>("_2_a", 0);
    a->rawExpression = "a";
    auto aName = make_shared<PropertyExpression>(STRING, "name", 1, move(a));
    auto csvLine1 = make_shared<VariableExpression>(CSV_LINE_EXTRACT, STRING, "_1_csvLine[1]");
    csvLine1->rawExpression = "csvLine[1]";
    auto expectedWhere = make_shared<Expression>(EQUALS, BOOL, move(aName), move(csvLine1));
    expectedWhere->rawExpression = "a.name = csvLine[1]";

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
    auto& leftANDExpr = matchStatement->whereExpression->children[0];
    auto& rightANDExpr = matchStatement->whereExpression->children[1];
    ASSERT_EQ(DATE, leftANDExpr->children[0]->dataType);
    ASSERT_EQ(DATE, leftANDExpr->children[1]->dataType);
    ASSERT_EQ(DATE, rightANDExpr->children[0]->dataType);
    ASSERT_EQ(DATE, rightANDExpr->children[1]->dataType);
}
