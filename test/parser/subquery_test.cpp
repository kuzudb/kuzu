#include "gtest/gtest.h"

#include "src/parser/expression/include/parsed_subquery_expression.h"
#include "src/parser/include/parser.h"
#include "src/parser/query/include/regular_query.h"

using namespace graphflow::parser;

const string EMPTY = string();

class SubqueryTest : public ::testing::Test {

public:
    static vector<unique_ptr<PatternElement>> makeEmptyPatternElements() {
        auto expectNode = make_unique<NodePattern>(
            string(), string(), vector<pair<string, unique_ptr<ParsedExpression>>>{});
        auto expectPElements = vector<unique_ptr<PatternElement>>();
        expectPElements.emplace_back(make_unique<PatternElement>(move(expectNode)));
        return expectPElements;
    }
};

TEST_F(SubqueryTest, ExistsTest) {
    auto existentialExpression =
        make_unique<ParsedSubqueryExpression>(makeEmptyPatternElements(), EMPTY);
    auto expectedExpression =
        make_unique<ParsedExpression>(NOT, std::move(existentialExpression), EMPTY);

    string input = "MATCH () WHERE NOT EXISTS { MATCH () } RETURN COUNT(*);";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getReadingClause(0);
    ASSERT_TRUE(*expectedExpression == *matchClause.getWhereClause());
}

TEST_F(SubqueryTest, NestedExistsTest) {
    auto secondInnerSubquery =
        make_unique<ParsedSubqueryExpression>(makeEmptyPatternElements(), EMPTY);
    auto expectedExpression =
        make_unique<ParsedSubqueryExpression>(makeEmptyPatternElements(), EMPTY);
    expectedExpression->setWhereClause(std::move(secondInnerSubquery));
    string input = "MATCH () WHERE EXISTS { MATCH () WHERE EXISTS { MATCH () } } RETURN COUNT(*);";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getReadingClause(0);
    ASSERT_TRUE(*expectedExpression == *matchClause.getWhereClause());
}
