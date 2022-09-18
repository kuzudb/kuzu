#include "gtest/gtest.h"

#include "src/parser/expression/include/parsed_subquery_expression.h"
#include "src/parser/include/parser.h"
#include "src/parser/query/include/regular_query.h"

using namespace graphflow::parser;

const string EMPTY = string();

class SubqueryTest : public ::testing::Test {

public:
    static unique_ptr<MatchClause> makeEmptyMatchClause() {
        auto expectNode = make_unique<NodePattern>(
            string(), string(), vector<pair<string, unique_ptr<ParsedExpression>>>{});
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
    auto innerQuery = make_unique<SingleQuery>();
    innerQuery->addReadingClause(makeEmptyMatchClause());
    innerQuery->setReturnClause(makeReturnStarClause());

    auto existentialExpression =
        make_unique<ParsedSubqueryExpression>(EXISTENTIAL_SUBQUERY, move(innerQuery), EMPTY);
    auto expectedExpression =
        make_unique<ParsedExpression>(NOT, move(existentialExpression), EMPTY);

    string input = "MATCH () WHERE NOT EXISTS { MATCH () RETURN * } RETURN COUNT(*);";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getReadingClause(0);
    ASSERT_TRUE(*expectedExpression == *matchClause.getWhereClause());
}

TEST_F(SubqueryTest, NestedExistsTest) {
    auto secondInnerQuery = make_unique<SingleQuery>();
    secondInnerQuery->addReadingClause(makeEmptyMatchClause());
    secondInnerQuery->setReturnClause(makeReturnStarClause());

    auto innerQuery = make_unique<SingleQuery>();
    innerQuery->setReturnClause(makeReturnStarClause());
    auto match = makeEmptyMatchClause();
    match->setWhereClause(
        make_unique<ParsedSubqueryExpression>(EXISTENTIAL_SUBQUERY, move(secondInnerQuery), EMPTY));
    innerQuery->addReadingClause(move(match));

    auto expectedExpression =
        make_unique<ParsedSubqueryExpression>(EXISTENTIAL_SUBQUERY, move(innerQuery), EMPTY);

    string input = "MATCH () WHERE EXISTS { MATCH () WHERE EXISTS { MATCH () RETURN * } RETURN "
                   "* } RETURN COUNT(*);";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getReadingClause(0);
    ASSERT_TRUE(*expectedExpression == *matchClause.getWhereClause());
}
