#include "gtest/gtest.h"
#include "test/parser/parser_test_utils.h"

#include "src/parser/include/parser.h"

using namespace graphflow::parser;

class ReturnWithTest : public ::testing::Test {

public:
    static unique_ptr<ParsedExpression> makeANameExpression() {
        auto expression = make_unique<ParsedExpression>(PROPERTY, "name", EMPTY);
        expression->children.push_back(make_unique<ParsedExpression>(VARIABLE, "a", EMPTY));
        return expression;
    }

    static unique_ptr<ParsedExpression> makeAAgeExpression() {
        auto expression = make_unique<ParsedExpression>(PROPERTY, "age", EMPTY);
        expression->children.push_back(make_unique<ParsedExpression>(VARIABLE, "a", EMPTY));
        return expression;
    }
};

TEST_F(ReturnWithTest, ReturnCountStarTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    expressions.push_back(make_unique<ParsedExpression>(FUNCTION, "COUNT_STAR", EMPTY));
    auto returnStatement = make_unique<ReturnStatement>(move(expressions), false);

    graphflow::parser::Parser parser;
    string input = "MATCH () RETURN COUNT(*);";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*returnStatement, *singleQuery->returnStatement));
}

TEST_F(ReturnWithTest, ReturnStarAndPropertyTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    expressions.push_back(makeANameExpression());
    auto returnStatement = make_unique<ReturnStatement>(move(expressions), true);

    graphflow::parser::Parser parser;
    string input = "MATCH () RETURN *, a.name;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*returnStatement, *singleQuery->returnStatement));
}

TEST_F(ReturnWithTest, ReturnAliasTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    expressions.push_back(makeANameExpression());
    auto aName2 = makeANameExpression();
    aName2->alias = "whatever";
    expressions.push_back(move(aName2));
    auto returnStatement = make_unique<ReturnStatement>(move(expressions), false);

    graphflow::parser::Parser parser;
    string input = "MATCH () RETURN a.name, a.name AS whatever;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*returnStatement, *singleQuery->returnStatement));
}

TEST_F(ReturnWithTest, SingleWithTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    auto one = make_unique<ParsedExpression>(LITERAL_INT, "1", EMPTY);
    one->alias = "one";
    expressions.push_back(move(one));
    auto name = make_unique<ParsedExpression>(LITERAL_STRING, "\"Xiyang\"", EMPTY);
    name->alias = "name";
    expressions.push_back(move(name));
    auto withStatement = make_unique<WithStatement>(move(expressions), false);

    graphflow::parser::Parser parser;
    string input = "WITH 1 AS one, \"Xiyang\" AS name MATCH () RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->queryParts.size());
    ASSERT_TRUE(0u == singleQuery->queryParts[0]->matchStatements.size());
    ASSERT_TRUE(
        ParserTestUtils::equals(*withStatement, *singleQuery->queryParts[0]->withStatement));
}

TEST_F(ReturnWithTest, MultiMatchWithStarTest) {
    auto expressions = vector<unique_ptr<ParsedExpression>>();
    auto one = make_unique<ParsedExpression>(LITERAL_INT, "1", EMPTY);
    one->alias = "one";
    expressions.push_back(move(one));
    auto withStatement = make_unique<WithStatement>(move(expressions), true);

    graphflow::parser::Parser parser;
    string input = "MATCH () MATCH () WITH *, 1 AS one MATCH () RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->queryParts.size());
    ASSERT_TRUE(2u == singleQuery->queryParts[0]->matchStatements.size());
    ASSERT_TRUE(
        ParserTestUtils::equals(*withStatement, *singleQuery->queryParts[0]->withStatement));
}

TEST_F(ReturnWithTest, MultiWithWhereTest) {
    auto expressions1 = vector<unique_ptr<ParsedExpression>>();
    auto withStatement1 = make_unique<WithStatement>(move(expressions1), true);
    auto one1 = make_unique<ParsedExpression>(LITERAL_INT, "1", EMPTY);
    auto aAge1 = makeAAgeExpression();
    auto where1 = make_unique<ParsedExpression>(LESS_THAN, EMPTY, EMPTY, move(aAge1), move(one1));
    withStatement1->whereClause = move(where1);

    auto expressions2 = vector<unique_ptr<ParsedExpression>>();
    auto aAge2 = makeAAgeExpression();
    aAge2->alias = "newAge";
    expressions2.push_back(move(aAge2));
    auto withStatement2 = make_unique<WithStatement>(move(expressions2), false);
    auto ten2 = make_unique<ParsedExpression>(LITERAL_INT, "10", EMPTY);
    auto newAge = make_unique<ParsedExpression>(VARIABLE, "newAge", EMPTY);
    auto where2 = make_unique<ParsedExpression>(EQUALS, EMPTY, EMPTY, move(newAge), move(ten2));
    withStatement2->whereClause = move(where2);

    graphflow::parser::Parser parser;
    string input =
        "MATCH () WITH * WHERE a.age < 1 WITH a.age AS newAge WHERE newAge = 10 MATCH () RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(2u == singleQuery->queryParts.size());
    ASSERT_TRUE(1u == singleQuery->queryParts[0]->matchStatements.size());
    ASSERT_TRUE(
        ParserTestUtils::equals(*withStatement1, *singleQuery->queryParts[0]->withStatement));
    ASSERT_TRUE(0u == singleQuery->queryParts[1]->matchStatements.size());
    ASSERT_TRUE(
        ParserTestUtils::equals(*withStatement2, *singleQuery->queryParts[1]->withStatement));
}
