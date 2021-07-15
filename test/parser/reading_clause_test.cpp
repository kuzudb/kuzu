#include "gtest/gtest.h"
#include "test/parser/parser_test_utils.h"

#include "src/parser/include/parser.h"

using namespace graphflow::parser;

class ReadingClauseTest : public ::testing::Test {

public:
    static unique_ptr<RelPattern> makeRelPattern(
        const string& name, const string& type, ArrowDirection direction) {
        auto rel = make_unique<RelPattern>(name, type, "1" /*lowerBound*/, "1" /*upperBound*/);
        rel->arrowDirection = direction;
        return rel;
    }
};

TEST_F(ReadingClauseTest, LoadCSVBasicTest) {
    auto file = make_unique<ParsedExpression>(LITERAL_STRING, "\"file\"", string());
    auto loadCSV = make_unique<LoadCSVStatement>(move(file), "csvLine");

    string input = "LOAD CSV WITH HEADERS FROM \"file\" AS csvLine RETURN COUNT(*);";
    auto singleQuery = Parser::parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->readingStatements.size());
    ASSERT_TRUE(
        ParserTestUtils::equals(*loadCSV, (LoadCSVStatement&)*singleQuery->readingStatements[0]));
}

TEST_F(ReadingClauseTest, EmptyMatchTest) {
    auto expectNode = make_unique<NodePattern>(string(), string());
    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(make_unique<PatternElement>(move(expectNode)));
    auto expectedMatch = make_unique<MatchStatement>(move(expectPElements));

    string input = "MATCH () RETURN COUNT(*);";
    auto singleQuery = Parser::parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->readingStatements.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectedMatch, (MatchStatement&)(*singleQuery->readingStatements[0])));
}

TEST_F(ReadingClauseTest, MATCHSingleEdgeTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person");
    auto expectNodeB = make_unique<NodePattern>("b", "Student");
    auto expectRel = makeRelPattern("e1", "knows", RIGHT);
    auto expectPElementChain = make_unique<PatternElementChain>(move(expectRel), move(expectNodeB));
    auto expectPElement = make_unique<PatternElement>(move(expectNodeA));
    expectPElement->patternElementChains.push_back(move(expectPElementChain));
    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(move(expectPElement));
    auto expectedMatch = make_unique<MatchStatement>(move(expectPElements));

    string input = "MATCH (a:Person)-[e1:knows]->(b:Student) RETURN *;";
    auto singleQuery = Parser::parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->readingStatements.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectedMatch, (MatchStatement&)(*singleQuery->readingStatements[0])));
}

TEST_F(ReadingClauseTest, MATCHMultiEdgesTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person");
    auto expectNodeB = make_unique<NodePattern>("b", "Student");
    auto expectRel1 = makeRelPattern(string(), "knows", RIGHT);
    auto expectedPElementChain1 =
        make_unique<PatternElementChain>(move(expectRel1), move(expectNodeB));
    auto expectNodeC = make_unique<NodePattern>("c", "Student");
    auto expectRel2 = makeRelPattern("e2", "likes", LEFT);
    auto expectedPElementChain2 =
        make_unique<PatternElementChain>(move(expectRel2), move(expectNodeC));
    auto expectPElement = make_unique<PatternElement>(move(expectNodeA));
    expectPElement->patternElementChains.push_back(move(expectedPElementChain1));
    expectPElement->patternElementChains.push_back(move(expectedPElementChain2));
    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(move(expectPElement));
    auto expectedMatch = make_unique<MatchStatement>(move(expectPElements));

    string input = "MATCH (a:Person)-[:knows]->(b:Student)<-[e2:likes]-(c:Student) RETURN *;";
    auto singleQuery = Parser::parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->readingStatements.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectedMatch, (MatchStatement&)(*singleQuery->readingStatements[0])));
}

TEST_F(ReadingClauseTest, MATCHMultiElementsTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person");
    auto expectNodeB = make_unique<NodePattern>("b", "Student");
    auto expectRel1 = makeRelPattern(string(), "knows", RIGHT);
    auto expectedPElementChain1 =
        make_unique<PatternElementChain>(move(expectRel1), move(expectNodeB));
    auto expectPElement1 = make_unique<PatternElement>(move(expectNodeA));
    expectPElement1->patternElementChains.push_back(move(expectedPElementChain1));

    auto expectNodeB2 = make_unique<NodePattern>("b", string());
    auto expectNodeC = make_unique<NodePattern>("c", "Student");
    auto expectRel2 = makeRelPattern("e2", "likes", LEFT);
    auto expectedPElementChain2 =
        make_unique<PatternElementChain>(move(expectRel2), move(expectNodeC));
    auto expectPElement2 = make_unique<PatternElement>(move(expectNodeB2));
    expectPElement2->patternElementChains.push_back(move(expectedPElementChain2));

    auto expectPElements = vector<unique_ptr<PatternElement>>();
    expectPElements.emplace_back(move(expectPElement1));
    expectPElements.emplace_back(move(expectPElement2));
    auto expectedMatch = make_unique<MatchStatement>(move(expectPElements));

    string input = "MATCH (a:Person)-[:knows]->(b:Student), (b)<-[e2:likes]-(c:Student) RETURN *;";
    auto singleQuery = Parser::parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->readingStatements.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectedMatch, (MatchStatement&)(*singleQuery->readingStatements[0])));
}

TEST_F(ReadingClauseTest, MultiMatchTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person");
    auto expectNodeB = make_unique<NodePattern>("b", "Student");
    auto expectRel1 = makeRelPattern(string(), "knows", RIGHT);
    auto expectedPElementChain1 =
        make_unique<PatternElementChain>(move(expectRel1), move(expectNodeB));
    auto expectPElement1 = make_unique<PatternElement>(move(expectNodeA));
    expectPElement1->patternElementChains.push_back(move(expectedPElementChain1));
    auto expectPElements1 = vector<unique_ptr<PatternElement>>();
    expectPElements1.emplace_back(move(expectPElement1));
    auto expectedMatch1 = make_unique<MatchStatement>(move(expectPElements1));

    auto expectNodeB2 = make_unique<NodePattern>("b", string());
    auto expectNodeC = make_unique<NodePattern>("c", "Student");
    auto expectRel2 = makeRelPattern("e2", "likes", LEFT);
    auto expectedPElementChain2 =
        make_unique<PatternElementChain>(move(expectRel2), move(expectNodeC));
    auto expectPElement2 = make_unique<PatternElement>(move(expectNodeB2));
    expectPElement2->patternElementChains.push_back(move(expectedPElementChain2));
    auto expectPElements2 = vector<unique_ptr<PatternElement>>();
    expectPElements2.emplace_back(move(expectPElement2));
    auto expectedMatch2 = make_unique<MatchStatement>(move(expectPElements2));

    string input =
        "MATCH (a:Person)-[:knows]->(b:Student) MATCH (b)<-[e2:likes]-(c:Student) RETURN *;";
    auto singleQuery = Parser::parseQuery(input);
    ASSERT_TRUE(2u == singleQuery->readingStatements.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectedMatch1, (MatchStatement&)(*singleQuery->readingStatements[0])));
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectedMatch2, (MatchStatement&)(*singleQuery->readingStatements[1])));
}
