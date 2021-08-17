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

TEST_F(BinderTest, BasicSubqueryTest) {
    NiceMock<TinySnbCatalog> catalog;
    catalog.setUp();

    auto query =
        "MATCH (a:person) WHERE EXISTS { MATCH (a)-[:knows]->(:person) RETURN * } RETURN COUNT(*)";
    auto boundQuery = getBoundQuery(query, catalog);
    GF_ASSERT(boundQuery->boundReadingStatements.size() == 1 &&
              boundQuery->boundReadingStatements[0]->statementType == MATCH_STATEMENT);
    auto& match = (BoundMatchStatement&)*boundQuery->boundReadingStatements[0];
    GF_ASSERT(match.whereExpression->expressionType == EXISTENTIAL_SUBQUERY);
    auto& subquery =
        *static_pointer_cast<ExistentialSubqueryExpression>(match.whereExpression)->getSubquery();
    GF_ASSERT(subquery.boundReadingStatements.size() == 1 &&
              subquery.boundReadingStatements[0]->statementType == MATCH_STATEMENT);
    auto& innerMatch = (BoundMatchStatement&)*subquery.boundReadingStatements[0];
    GF_ASSERT(innerMatch.queryGraph->getNumQueryNodes() == 1);
    GF_ASSERT(innerMatch.queryGraph->getNumQueryRels() == 1);
}

TEST_F(BinderTest, NestedSubqueryTest) {
    NiceMock<TinySnbCatalog> catalog;
    catalog.setUp();

    auto query = "MATCH (a:person) WHERE EXISTS { MATCH (a)-[:knows]->(b:person) WHERE NOT EXISTS "
                 "{ MATCH (b)-[:workAt]->(c:organisation), (b)-[:knows]->(d:person) RETURN * } "
                 "RETURN * } OR a.age > 5 RETURN COUNT(*)";
    auto boundQuery = getBoundQuery(query, catalog);
    auto& match = (BoundMatchStatement&)*boundQuery->boundReadingStatements[0];
    GF_ASSERT(match.whereExpression->expressionType == OR);
    auto leftExpr = match.whereExpression->children[0];
    GF_ASSERT(leftExpr->expressionType == EXISTENTIAL_SUBQUERY);
    auto& subquery = *static_pointer_cast<ExistentialSubqueryExpression>(leftExpr)->getSubquery();
    auto& innerMatch = (BoundMatchStatement&)*subquery.boundReadingStatements[0];
    GF_ASSERT(innerMatch.whereExpression->children[0]->expressionType == EXISTENTIAL_SUBQUERY);
    auto& innerSubquery =
        *static_pointer_cast<ExistentialSubqueryExpression>(innerMatch.whereExpression->children[0])
             ->getSubquery();
    GF_ASSERT(innerSubquery.boundReadingStatements.size() == 1 &&
              innerSubquery.boundReadingStatements[0]->statementType == MATCH_STATEMENT);
    auto& innerInnerMatch = (BoundMatchStatement&)*innerSubquery.boundReadingStatements[0];
    GF_ASSERT(innerInnerMatch.queryGraph->getNumQueryNodes() == 2);
    GF_ASSERT(innerInnerMatch.queryGraph->getNumQueryRels() == 2);
}
