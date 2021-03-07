#include "gtest/gtest.h"

#include "src/parser/include/parser.h"

using namespace graphflow::parser;
using namespace graphflow::common;

class ParserTest : public :: testing::Test {

public:

    unique_ptr<NodePattern> makeNodePattern(string name, string label) {
        auto node = make_unique<NodePattern>();
        node->setName(name);
        node->setLabel(label);
        return node;
    }

    unique_ptr<RelPattern> makeRelPattern(string name, string type, Direction direction) {
        auto rel = make_unique<RelPattern>();
        rel->setName(name);
        rel->setType(type);
        rel->setDirection(direction);
        return rel;
    }

    unique_ptr<PatternElementChain> makePatternElementChain(unique_ptr<RelPattern> rel,
            unique_ptr<NodePattern> node) {
        auto patternElementChain = make_unique<PatternElementChain>();
        patternElementChain->setNodePattern(move(node));
        patternElementChain->setRelPattern(move(rel));
        return patternElementChain;
    }

    unique_ptr<PatternElement> makePatternElement(unique_ptr<NodePattern> nodePattern) {
        auto patternElement = make_unique<PatternElement>();
        patternElement->setNodePattern(move(nodePattern));
        return patternElement;
    }

};

TEST_F(ParserTest, MATCHSingleElementSingleNodeNoLabelTest) {
    auto expectedNode = makeNodePattern("a", string());
    auto expectedPatternElement = makePatternElement(move(expectedNode));
    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (a);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MATCHSingleElementSingleNodeSingleLabelTest) {
    auto expectedNode = makeNodePattern("a", "Person");
    auto expectedPatternElement = makePatternElement(move(expectedNode));
    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MATCHSingleElementAnonymousNodeSingleLabelTest) {
    auto expectedNode = makeNodePattern(string(), "Person");
    auto expectedPatternElement = makePatternElement(move(expectedNode));
    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (:Person);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MATCHSingleElementSingleEdgeNoTypeTest) {
    auto expectedNodeA = makeNodePattern("a", "Person");
    auto expectedNodeB = makeNodePattern("b", "Student");
    auto expectedEdge = makeRelPattern("e1", string(), FWD);
    auto expectedPatternElementChain = makePatternElementChain(move(expectedEdge), move(expectedNodeB));
    auto expectedPatternElement = makePatternElement(move(expectedNodeA));
    expectedPatternElement->addPatternElementChain(move(expectedPatternElementChain));
    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[e1]->(b:Student);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MATCHSingleElementSingleEdgeSingleTypeTest) {
    auto expectedNodeA = makeNodePattern("a", "Person");
    auto expectedNodeB = makeNodePattern("b", "Student");
    auto expectedEdge = makeRelPattern("e1", "knows", FWD);
    auto expectedPatternElementChain = makePatternElementChain(move(expectedEdge), move(expectedNodeB));
    auto expectedPatternElement = makePatternElement(move(expectedNodeA));
    expectedPatternElement->addPatternElementChain(move(expectedPatternElementChain));
    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[e1:knows]->(b:Student);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MATCHSingleElementAnonymousEdgeMultiTypesTest) {
    auto expectedNodeA = makeNodePattern("a", "Person");
    auto expectedNodeB = makeNodePattern("b", "Student");
    auto expectedEdge = makeRelPattern(string(), "knows", FWD);
    auto expectedPatternElementChain = makePatternElementChain(move(expectedEdge), move(expectedNodeB));
    auto expectedPatternElement = makePatternElement(move(expectedNodeA));
    expectedPatternElement->addPatternElementChain(move(expectedPatternElementChain));
    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[:knows]->(b:Student);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MATCHSingleElementMultiEdgesTest) {
    auto expectedNodeA = makeNodePattern("a", "Person");
    auto expectedNodeB = makeNodePattern("b", "Student");
    auto expectedEdge1 = makeRelPattern(string(), "knows", FWD);
    auto expectedPatternElementChain1 = makePatternElementChain(move(expectedEdge1), move(expectedNodeB));
    auto expectedNodeC = makeNodePattern("c", "Student");
    auto expectedEdge2 = makeRelPattern("e2", "likes", BWD);
    auto expectedPatternElementChain2 = makePatternElementChain(move(expectedEdge2), move(expectedNodeC));
    auto expectedPatternElement = makePatternElement(move(expectedNodeA));
    expectedPatternElement->addPatternElementChain(move(expectedPatternElementChain1));
    expectedPatternElement->addPatternElementChain(move(expectedPatternElementChain2));
    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[:knows]->(b:Student)<-[e2:likes]-(c:Student);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MATCHMultiElementsTest) {
    auto expectedNodeA = makeNodePattern("a", "Person");
    auto expectedNodeB = makeNodePattern("b", "Student");
    auto expectedEdge1 = makeRelPattern(string(), "knows", FWD);
    auto expectedPatternElementChain1 = makePatternElementChain(move(expectedEdge1), move(expectedNodeB));
    auto expectedPatternElement1 = makePatternElement(move(expectedNodeA));
    expectedPatternElement1->addPatternElementChain(move(expectedPatternElementChain1));

    auto expectedNodeBDup = makeNodePattern("b", string());
    auto expectedNodeC = makeNodePattern("c", "Student");
    auto expectedEdge2 = makeRelPattern("e2", "likes", BWD);
    auto expectedPatternElementChain2 = makePatternElementChain(move(expectedEdge2), move(expectedNodeC));
    auto expectedPatternElement2 = makePatternElement(move(expectedNodeBDup));
    expectedPatternElement2->addPatternElementChain(move(expectedPatternElementChain2));

    vector<unique_ptr<PatternElement>> expectedPatternElements;
    expectedPatternElements.push_back(move(expectedPatternElement1));
    expectedPatternElements.push_back(move(expectedPatternElement2));
    auto expectedMatchStatement = make_unique<MatchStatement>(move(expectedPatternElements));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[:knows]->(b:Student), (b)<-[e2:likes]-(c:Student);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ParserTest, MultiMatchTest) {
    auto expectedNodeA = makeNodePattern("a", "Person");
    auto expectedNodeB = makeNodePattern("b", "Student");
    auto expectedEdge1 = makeRelPattern(string(), "knows", FWD);
    auto expectedPatternElementChain1 = makePatternElementChain(move(expectedEdge1), move(expectedNodeB));
    auto expectedPatternElement1 = makePatternElement(move(expectedNodeA));
    expectedPatternElement1->addPatternElementChain(move(expectedPatternElementChain1));
    vector<unique_ptr<PatternElement>> expectedPatternElements1;
    expectedPatternElements1.push_back(move(expectedPatternElement1));
    auto expectedMatchStatement1 = make_unique<MatchStatement>(move(expectedPatternElements1));

    auto expectedNodeBDup = makeNodePattern("b", string());
    auto expectedNodeC = makeNodePattern("c", "Student");
    auto expectedEdge2 = makeRelPattern("e2", "likes", BWD);
    auto expectedPatternElementChain2 = makePatternElementChain(move(expectedEdge2), move(expectedNodeC));
    auto expectedPatternElement2 = makePatternElement(move(expectedNodeBDup));
    expectedPatternElement2->addPatternElementChain(move(expectedPatternElementChain2));
    vector<unique_ptr<PatternElement>> expectedPatternElements2;
    expectedPatternElements2.push_back(move(expectedPatternElement2));
    auto expectedMatchStatement2 = make_unique<MatchStatement>(move(expectedPatternElements2));

    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement1));
    expectedSingleQuery->addMatchStatement(move(expectedMatchStatement2));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:Person)-[:knows]->(b:Student) MATCH (b)<-[e2:likes]-(c:Student);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}
