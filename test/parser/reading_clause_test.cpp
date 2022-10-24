#include "gtest/gtest.h"

#include "src/parser/include/parser.h"
#include "src/parser/query/include/regular_query.h"

using namespace graphflow::parser;

class ReadingClauseTest : public ::testing::Test {

public:
    static unique_ptr<RelPattern> makeRelPattern(
        const string& name, const string& type, ArrowDirection direction) {
        return make_unique<RelPattern>(name, type, "1" /*lowerBound*/, "1" /*upperBound*/,
            direction, vector<pair<string, unique_ptr<ParsedExpression>>>{});
    }

    vector<pair<string, unique_ptr<ParsedExpression>>> getDummyProperties() {
        return vector<pair<string, unique_ptr<ParsedExpression>>>{};
    }
};

TEST_F(ReadingClauseTest, EmptyMatchTest) {
    auto expectNode = make_unique<NodePattern>(string(), string(), getDummyProperties());
    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(make_unique<PatternElement>(move(expectNode)));
    auto expectedMatch = make_unique<MatchClause>(move(expectPElements));

    string input = "MATCH () RETURN COUNT(*);";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(1u == regularQuery->getSingleQuery(0)->getNumReadingClauses());
    ASSERT_TRUE(*expectedMatch == *regularQuery->getSingleQuery(0)->getReadingClause(0));
}

TEST_F(ReadingClauseTest, MATCHSingleEdgeTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person", getDummyProperties());
    auto expectNodeB = make_unique<NodePattern>("b", "Student", getDummyProperties());
    auto expectRel = makeRelPattern("e1", "knows", RIGHT);
    auto expectPElementChain = make_unique<PatternElementChain>(move(expectRel), move(expectNodeB));
    auto expectPElement = make_unique<PatternElement>(move(expectNodeA));
    expectPElement->addPatternElementChain(move(expectPElementChain));
    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(move(expectPElement));
    auto expectedMatch = make_unique<MatchClause>(move(expectPElements));

    string input = "MATCH (a:Person)-[e1:knows]->(b:Student) RETURN *;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(1u == regularQuery->getSingleQuery(0)->getNumReadingClauses());
    ASSERT_TRUE(*expectedMatch == *regularQuery->getSingleQuery(0)->getReadingClause(0));
}

TEST_F(ReadingClauseTest, MATCHMultiEdgesTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person", getDummyProperties());
    auto expectNodeB = make_unique<NodePattern>("b", "Student", getDummyProperties());
    auto expectRel1 = makeRelPattern(string(), "knows", RIGHT);
    auto expectedPElementChain1 =
        make_unique<PatternElementChain>(move(expectRel1), move(expectNodeB));
    auto expectNodeC = make_unique<NodePattern>("c", "Student", getDummyProperties());
    auto expectRel2 = makeRelPattern("e2", "likes", LEFT);
    auto expectedPElementChain2 =
        make_unique<PatternElementChain>(move(expectRel2), move(expectNodeC));
    auto expectPElement = make_unique<PatternElement>(move(expectNodeA));
    expectPElement->addPatternElementChain(move(expectedPElementChain1));
    expectPElement->addPatternElementChain(move(expectedPElementChain2));
    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(move(expectPElement));
    auto expectedMatch = make_unique<MatchClause>(move(expectPElements));

    string input = "MATCH (a:Person)-[:knows]->(b:Student)<-[e2:likes]-(c:Student) RETURN *;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(1u == regularQuery->getSingleQuery(0)->getNumReadingClauses());
    ASSERT_TRUE(*expectedMatch == *regularQuery->getSingleQuery(0)->getReadingClause(0));
}

TEST_F(ReadingClauseTest, MATCHMultiElementsTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person", getDummyProperties());
    auto expectNodeB = make_unique<NodePattern>("b", "Student", getDummyProperties());
    auto expectRel1 = makeRelPattern(string(), "knows", RIGHT);
    auto expectedPElementChain1 =
        make_unique<PatternElementChain>(move(expectRel1), move(expectNodeB));
    auto expectPElement1 = make_unique<PatternElement>(move(expectNodeA));
    expectPElement1->addPatternElementChain(move(expectedPElementChain1));

    auto expectNodeB2 = make_unique<NodePattern>("b", string(), getDummyProperties());
    auto expectNodeC = make_unique<NodePattern>("c", "Student", getDummyProperties());
    auto expectRel2 = makeRelPattern("e2", "likes", LEFT);
    auto expectedPElementChain2 =
        make_unique<PatternElementChain>(move(expectRel2), move(expectNodeC));
    auto expectPElement2 = make_unique<PatternElement>(move(expectNodeB2));
    expectPElement2->addPatternElementChain(move(expectedPElementChain2));

    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(move(expectPElement1));
    expectPElements.emplace_back(move(expectPElement2));
    auto expectedMatch = make_unique<MatchClause>(move(expectPElements));

    string input = "MATCH (a:Person)-[:knows]->(b:Student), (b)<-[e2:likes]-(c:Student) RETURN *;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(1u == regularQuery->getSingleQuery(0)->getNumReadingClauses());
    ASSERT_TRUE(*expectedMatch == *regularQuery->getSingleQuery(0)->getReadingClause(0));
}

TEST_F(ReadingClauseTest, MultiMatchTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person", getDummyProperties());
    auto expectNodeB = make_unique<NodePattern>("b", "Student", getDummyProperties());
    auto expectRel1 = makeRelPattern(string(), "knows", RIGHT);
    auto expectedPElementChain1 =
        make_unique<PatternElementChain>(move(expectRel1), move(expectNodeB));
    auto expectPElement1 = make_unique<PatternElement>(move(expectNodeA));
    expectPElement1->addPatternElementChain(move(expectedPElementChain1));
    auto expectPElements1 = vector<unique_ptr<PatternElement>>();
    expectPElements1.emplace_back(move(expectPElement1));
    auto expectedMatch1 = make_unique<MatchClause>(move(expectPElements1));

    auto expectNodeB2 = make_unique<NodePattern>("b", string(), getDummyProperties());
    auto expectNodeC = make_unique<NodePattern>("c", "Student", getDummyProperties());
    auto expectRel2 = makeRelPattern("e2", "likes", LEFT);
    auto expectedPElementChain2 =
        make_unique<PatternElementChain>(move(expectRel2), move(expectNodeC));
    auto expectPElement2 = make_unique<PatternElement>(move(expectNodeB2));
    expectPElement2->addPatternElementChain(move(expectedPElementChain2));
    auto expectPElements2 = vector<unique_ptr<PatternElement>>();
    expectPElements2.emplace_back(move(expectPElement2));
    auto expectedMatch2 = make_unique<MatchClause>(move(expectPElements2));

    string input =
        "MATCH (a:Person)-[:knows]->(b:Student) MATCH (b)<-[e2:likes]-(c:Student) RETURN *;";
    auto parsedQuery = Parser::parseQuery(input);
    auto regularQuery = reinterpret_cast<RegularQuery*>(parsedQuery.get());
    ASSERT_TRUE(2u == regularQuery->getSingleQuery(0)->getNumReadingClauses());
    ASSERT_TRUE(*expectedMatch1 == *regularQuery->getSingleQuery(0)->getReadingClause(0));
    ASSERT_TRUE(*expectedMatch2 == *regularQuery->getSingleQuery(0)->getReadingClause(1));
}
