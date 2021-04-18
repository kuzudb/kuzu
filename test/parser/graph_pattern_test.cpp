#include "gtest/gtest.h"
#include "test/parser/parser_test_utils.h"

#include "src/parser/include/parser.h"

using namespace graphflow::parser;

class GraphPatternTest : public ::testing::Test {

public:
    unique_ptr<RelPattern> makeRelPattern(string name, string type, ArrowDirection direction) {
        auto rel = make_unique<RelPattern>(name, type);
        rel->arrowDirection = direction;
        return rel;
    }
};

TEST_F(GraphPatternTest, EmptyMatchTest) {
    auto expectNode = make_unique<NodePattern>(string(), string());
    auto expectPElement = make_unique<PatternElement>(move(expectNode));

    graphflow::parser::Parser parser;
    string input = "MATCH () RETURN COUNT(*);";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->matchStatements.size());
    ASSERT_TRUE(1u == singleQuery->matchStatements[0]->graphPattern.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectPElement, *singleQuery->matchStatements[0]->graphPattern[0]));
}

TEST_F(GraphPatternTest, MATCHSingleEdgeTest) {
    auto expectNodeA = make_unique<NodePattern>("a", "Person");
    auto expectNodeB = make_unique<NodePattern>("b", "Student");
    auto expectRel = makeRelPattern("e1", "knows", RIGHT);
    auto expectPElementChain = make_unique<PatternElementChain>(move(expectRel), move(expectNodeB));
    auto expectPElement = make_unique<PatternElement>(move(expectNodeA));
    expectPElement->patternElementChains.push_back(move(expectPElementChain));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[e1:knows]->(b:Student) RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->matchStatements.size());
    ASSERT_TRUE(1u == singleQuery->matchStatements[0]->graphPattern.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectPElement, *singleQuery->matchStatements[0]->graphPattern[0]));
}

TEST_F(GraphPatternTest, MATCHMultiEdgesTest) {
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

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[:knows]->(b:Student)<-[e2:likes]-(c:Student) RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->matchStatements.size());
    ASSERT_TRUE(1u == singleQuery->matchStatements[0]->graphPattern.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectPElement, *singleQuery->matchStatements[0]->graphPattern[0]));
}

TEST_F(GraphPatternTest, MATCHMultiElementsTest) {
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

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[:knows]->(b:Student), (b)<-[e2:likes]-(c:Student) RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(1u == singleQuery->matchStatements.size());
    ASSERT_TRUE(2u == singleQuery->matchStatements[0]->graphPattern.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectPElement1, *singleQuery->matchStatements[0]->graphPattern[0]));
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectPElement2, *singleQuery->matchStatements[0]->graphPattern[1]));
}

TEST_F(GraphPatternTest, MultiMatchTest) {
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

    graphflow::parser::Parser parser;
    string input =
        "MATCH (a:Person)-[:knows]->(b:Student) MATCH (b)<-[e2:likes]-(c:Student) RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(2u == singleQuery->matchStatements.size());
    ASSERT_TRUE(1u == singleQuery->matchStatements[0]->graphPattern.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectPElement1, *singleQuery->matchStatements[0]->graphPattern[0]));
    ASSERT_TRUE(1u == singleQuery->matchStatements[1]->graphPattern.size());
    ASSERT_TRUE(ParserTestUtils::equals(
        *expectPElement2, *singleQuery->matchStatements[1]->graphPattern[0]));
}
