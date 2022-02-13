#include "gtest/gtest.h"

#include "src/parser/expression/include/parsed_subquery_expression.h"
#include "src/parser/include/parser.h"

using namespace graphflow::parser;

const string EMPTY = string();

class SubqueryTest : public ::testing::Test {

public:
    static unique_ptr<MatchClause> makeEmptyMatchClause() {
        auto expectNode = make_unique<NodePattern>(string(), string());
        auto expectPElements = vector<unique_ptr<PatternElement>>();
        expectPElements.emplace_back(make_unique<PatternElement>(move(expectNode)));
        return make_unique<MatchClause>(move(expectPElements));
    }

    static unique_ptr<ReturnClause> makeReturnStarClause() {
        auto expressions = vector<unique_ptr<ParsedExpression>>();
        return make_unique<ReturnClause>(make_unique<ProjectionBody>(
            false /* isDistinct */, true /* containsStar */, move(expressions)));
    }
};

TEST_F(SubqueryTest, ExistsTest) {
    auto innerQuery = make_unique<SingleQuery>(makeReturnStarClause());
    innerQuery->addMatchClause(makeEmptyMatchClause());

    auto existentialExpression =
        make_unique<ParsedSubqueryExpression>(EXISTENTIAL_SUBQUERY, move(innerQuery), EMPTY);
    auto expectedExpression =
        make_unique<ParsedExpression>(NOT, move(existentialExpression), EMPTY);

    string input = "MATCH () WHERE NOT EXISTS { MATCH () RETURN * } RETURN COUNT(*);";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*expectedExpression == *matchClause.getWhereClause());
}

TEST_F(SubqueryTest, NestedExistsTest) {
    auto secondInnerQuery = make_unique<SingleQuery>(makeReturnStarClause());
    secondInnerQuery->addMatchClause(makeEmptyMatchClause());

    auto innerQuery = make_unique<SingleQuery>(makeReturnStarClause());
    auto match = makeEmptyMatchClause();
    match->setWhereClause(
        make_unique<ParsedSubqueryExpression>(EXISTENTIAL_SUBQUERY, move(secondInnerQuery), EMPTY));
    innerQuery->addMatchClause(move(match));

    auto expectedExpression =
        make_unique<ParsedSubqueryExpression>(EXISTENTIAL_SUBQUERY, move(innerQuery), EMPTY);

    string input = "MATCH () WHERE EXISTS { MATCH () WHERE EXISTS { MATCH () RETURN * } RETURN "
                   "* } RETURN COUNT(*);";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*expectedExpression == *matchClause.getWhereClause());
}
