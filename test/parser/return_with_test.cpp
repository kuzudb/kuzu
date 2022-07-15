#include "gtest/gtest.h"

#include "src/parser/expression/include/parsed_function_expression.h"
#include "src/parser/expression/include/parsed_literal_expression.h"
#include "src/parser/expression/include/parsed_property_expression.h"
#include "src/parser/expression/include/parsed_variable_expression.h"
#include "src/parser/include/parser.h"
#include "src/parser/query/include/regular_query.h"

using namespace graphflow::parser;

const string EMPTY = string();

class ReturnWithTest : public ::testing::Test {

public:
    static unique_ptr<ParsedExpression> makeANameExpression() {
        auto child = make_unique<ParsedVariableExpression>("a", EMPTY);
        return make_unique<ParsedPropertyExpression>(PROPERTY, "name", move(child), EMPTY);
    }

    static unique_ptr<ParsedExpression> makeAAgeExpression() {
        auto child = make_unique<ParsedVariableExpression>("a", EMPTY);
        return make_unique<ParsedPropertyExpression>(PROPERTY, "age", move(child), EMPTY);
    }
};

TEST_F(ReturnWithTest, ReturnCountStarTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    expressions.push_back(make_unique<ParsedFunctionExpression>("COUNT_STAR", EMPTY));
    auto returnClause = make_unique<ReturnClause>(make_unique<ProjectionBody>(
        false /* isDistinct */, false /* containsStar */, move(expressions)));

    string input = "MATCH () RETURN COUNT(*);";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(*returnClause == *regularQuery->getSingleQuery(0)->getReturnClause());
}

TEST_F(ReturnWithTest, ReturnStarAndPropertyTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    expressions.push_back(makeANameExpression());
    auto returnClause = make_unique<ReturnClause>(make_unique<ProjectionBody>(
        false /* isDistinct */, true /* containsStar */, move(expressions)));

    string input = "MATCH () RETURN *, a.name;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(*returnClause == *regularQuery->getSingleQuery(0)->getReturnClause());
}

TEST_F(ReturnWithTest, ReturnAliasTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    expressions.push_back(makeANameExpression());
    auto aName2 = makeANameExpression();
    aName2->setAlias("whatever");
    expressions.push_back(move(aName2));
    auto returnClause = make_unique<ReturnClause>(make_unique<ProjectionBody>(
        false /* isDistinct */, false /* containsStar */, move(expressions)));

    string input = "MATCH () RETURN a.name, a.name AS whatever;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(*returnClause == *regularQuery->getSingleQuery(0)->getReturnClause());
}

TEST_F(ReturnWithTest, ReturnLimitTest) {
    vector<unique_ptr<ParsedExpression>> projectionExpressions;
    projectionExpressions.push_back(makeANameExpression());
    auto projectionBody = make_unique<ProjectionBody>(
        false /* isDistinct */, false /* containsStar */, move(projectionExpressions));
    projectionBody->setLimitExpression(
        make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)10), EMPTY));
    auto returnClause = make_unique<ReturnClause>(move(projectionBody));
    string input = "MATCH () RETURN a.name LIMIT 10;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(*returnClause == *regularQuery->getSingleQuery(0)->getReturnClause());
}

TEST_F(ReturnWithTest, SingleWithTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    auto one = make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)1), EMPTY);
    one->setAlias("one");
    expressions.push_back(move(one));
    auto name = make_unique<ParsedLiteralExpression>(make_unique<Literal>(string("Xiyang")), EMPTY);
    name->setAlias("name");
    expressions.push_back(move(name));
    auto withClause = make_unique<WithClause>(make_unique<ProjectionBody>(
        false /* isDistinct */, false /* containsStar */, move(expressions)));

    string input = "WITH 1 AS one, \"Xiyang\" AS name MATCH () RETURN *;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(1u == regularQuery->getSingleQuery(0)->getNumQueryParts());
    ASSERT_TRUE(regularQuery->getSingleQuery(0)->getQueryPart(0)->getNumMatchClauses() == 0);
    ASSERT_TRUE(*withClause == *regularQuery->getSingleQuery(0)->getQueryPart(0)->getWithClause());
}

TEST_F(ReturnWithTest, MultiMatchWithStarTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    auto one = make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)1), EMPTY);
    one->setAlias("one");
    expressions.push_back(move(one));
    auto withClause = make_unique<WithClause>(make_unique<ProjectionBody>(
        false /* isDistinct */, true /* containsStar */, move(expressions)));

    string input = "MATCH () MATCH () WITH *, 1 AS one MATCH () RETURN *;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(1u == regularQuery->getSingleQuery(0)->getNumQueryParts());
    ASSERT_TRUE(2u == regularQuery->getSingleQuery(0)->getQueryPart(0)->getNumMatchClauses());
    ASSERT_TRUE(*withClause == *regularQuery->getSingleQuery(0)->getQueryPart(0)->getWithClause());
}

TEST_F(ReturnWithTest, MultiWithWhereTest) {
    auto expressions1 = vector<unique_ptr<ParsedExpression>>();
    auto withClause1 = make_unique<WithClause>(make_unique<ProjectionBody>(
        false /* isDistinct */, true /* containsStar */, move(expressions1)));
    auto one1 = make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)1), EMPTY);
    auto aAge1 = makeAAgeExpression();
    auto where1 = make_unique<ParsedExpression>(LESS_THAN, move(aAge1), move(one1), EMPTY);
    withClause1->setWhereExpression(move(where1));

    auto expressions2 = vector<unique_ptr<ParsedExpression>>();
    auto aAge2 = makeAAgeExpression();
    aAge2->setAlias("newAge");
    expressions2.push_back(move(aAge2));
    auto withClause2 = make_unique<WithClause>(make_unique<ProjectionBody>(
        false /* isDistinct */, false /* containsStar */, move(expressions2)));
    auto ten2 = make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)10), EMPTY);
    auto newAge = make_unique<ParsedVariableExpression>("newAge", EMPTY);
    auto where2 = make_unique<ParsedExpression>(EQUALS, move(newAge), move(ten2), EMPTY);
    withClause2->setWhereExpression(move(where2));

    string input =
        "MATCH () WITH * WHERE a.age < 1 WITH a.age AS newAge WHERE newAge = 10 MATCH () RETURN *;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(2u == regularQuery->getSingleQuery(0)->getNumQueryParts());
    ASSERT_TRUE(1u == regularQuery->getSingleQuery(0)->getQueryPart(0)->getNumMatchClauses());
    ASSERT_TRUE(*withClause1 == *regularQuery->getSingleQuery(0)->getQueryPart(0)->getWithClause());
    ASSERT_TRUE(regularQuery->getSingleQuery(0)->getQueryPart(1)->getNumMatchClauses() == 0u);
    ASSERT_TRUE(*withClause2 == *regularQuery->getSingleQuery(0)->getQueryPart(1)->getWithClause());
}
