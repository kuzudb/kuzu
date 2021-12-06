#include "gtest/gtest.h"
#include "test/parser/parser_test_utils.h"

#include "src/parser/include/parser.h"

using namespace graphflow::parser;

class SubqueryTest : public ::testing::Test {

public:
    static unique_ptr<MatchStatement> makeEmptyMatchStatement() {
        auto expectNode = make_unique<NodePattern>(string(), string());
        auto expectPElements = vector<unique_ptr<PatternElement>>();
        expectPElements.emplace_back(make_unique<PatternElement>(move(expectNode)));
        return make_unique<MatchStatement>(move(expectPElements));
    }

    static unique_ptr<ReturnStatement> makeReturnStarStatement() {
        auto expressions = vector<unique_ptr<ParsedExpression>>();
        return make_unique<ReturnStatement>(make_unique<ProjectionBody>(true, move(expressions)));
    }
};

TEST_F(SubqueryTest, ExistsTest) {
    auto innerQuery = make_unique<SingleQuery>();
    innerQuery->matchStatements.push_back(makeEmptyMatchStatement());
    innerQuery->returnStatement = makeReturnStarStatement();

    auto existentialExpression =
        make_unique<ParsedExpression>(EXISTENTIAL_SUBQUERY, move(innerQuery), EMPTY);
    auto expectedExpression = make_unique<ParsedExpression>(NOT, EMPTY, EMPTY);
    expectedExpression->children.push_back(move(existentialExpression));

    string input = "MATCH () WHERE NOT EXISTS { MATCH () RETURN * } RETURN COUNT(*);";
    auto singleQuery = Parser::parseQuery(input);
    auto& matchStatement = (MatchStatement&)*singleQuery->matchStatements[0];
    ASSERT_TRUE(ParserTestUtils::equals(*expectedExpression, *matchStatement.whereClause));
}

TEST_F(SubqueryTest, NestedExistsTest) {
    auto secondInnerQuery = make_unique<SingleQuery>();
    secondInnerQuery->matchStatements.push_back(makeEmptyMatchStatement());
    secondInnerQuery->returnStatement = makeReturnStarStatement();

    auto innerQuery = make_unique<SingleQuery>();
    auto match = makeEmptyMatchStatement();
    match->whereClause =
        make_unique<ParsedExpression>(EXISTENTIAL_SUBQUERY, move(secondInnerQuery), EMPTY);
    innerQuery->matchStatements.push_back(move(match));
    innerQuery->returnStatement = makeReturnStarStatement();

    auto expectedExpression =
        make_unique<ParsedExpression>(EXISTENTIAL_SUBQUERY, move(innerQuery), EMPTY);

    string input = "MATCH () WHERE EXISTS { MATCH () WHERE EXISTS { MATCH () RETURN * } RETURN "
                   "* } RETURN COUNT(*);";
    auto singleQuery = Parser::parseQuery(input);
    auto& matchStatement = (MatchStatement&)*singleQuery->matchStatements[0];
    ASSERT_TRUE(ParserTestUtils::equals(*expectedExpression, *matchStatement.whereClause));
}
