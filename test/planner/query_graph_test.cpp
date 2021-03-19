#include "gtest/gtest.h"

#include "src/planner/include/binder.h"
#include "src/loader/include/graph_loader.h"
#include "src/parser/include/parser.h"

using namespace graphflow::loader;
using namespace graphflow::planner;

class QueryGraphTest : public :: testing::Test {

public:
    void SetUp() override {
        catalog = make_unique<Catalog>(createStringToNodeLabelMap(), createStringToRelLabelMap(),
            createSrcNodeLabelToRelLabelsVector(), createDstNodeLabelToRelLabelsVector(),
            createRelLabelToSrcNodeLabels(), createRelLabelToDstNodeLabels());
    }

private:
    stringToLabelMap_t createStringToNodeLabelMap() {
        stringToLabelMap_t map;
        map.insert({"person", 0});
        map.insert({"comment", 1});
        return map;
    }

    stringToLabelMap_t createStringToRelLabelMap() {
        stringToLabelMap_t map;
        map.insert({"knows", 0});
        map.insert({"likes", 1});
        return map;
    }

    vector<vector<label_t>> createSrcNodeLabelToRelLabelsVector() {
        vector<vector<label_t>> result;
        vector<label_t> personToRelLabel = {0, 1};
        vector<label_t> commentToRelLabel = {};
        result.push_back(personToRelLabel);
        result.push_back(commentToRelLabel);
        return result;
    }

    vector<vector<label_t>> createDstNodeLabelToRelLabelsVector() {
        vector<vector<label_t>> result;
        vector<label_t> personToRelLabel = {0};
        vector<label_t> commentToRelLabel = {1};
        result.push_back(personToRelLabel);
        result.push_back(commentToRelLabel);
        return result;
    }

    vector<vector<label_t>> createRelLabelToSrcNodeLabels() {
        vector<vector<label_t>> result;
        result.resize(2);
        vector<label_t> knowsToSrcLabels = {0};
        vector<label_t> likesToSrcLabels = {0};
        result.push_back(knowsToSrcLabels);
        result.push_back(likesToSrcLabels);
        return result;
    }

    vector<vector<label_t>> createRelLabelToDstNodeLabels() {
        vector<vector<label_t>> result;
        result.resize(2);
        vector<label_t> knowsToDstLabels = {0};
        vector<label_t> likesToDstLabels = {1};
        result.push_back(knowsToDstLabels);
        result.push_back(likesToDstLabels);
        return result;
    }

public:
    unique_ptr<Catalog> catalog;
};

TEST_F(QueryGraphTest, SingleEdgeTest) {
    auto expectedQueryNodeA = make_unique<QueryNode>("a", catalog->getNodeLabelFromString("person"));
    auto expectedQueryNodeB = make_unique<QueryNode>("b", catalog->getNodeLabelFromString("person"));
    auto expectedQueryRel = make_unique<QueryRel>("e1", catalog->getRelLabelFromString("knows"));
    expectedQueryRel->setSrcNode(expectedQueryNodeA.get());
    expectedQueryRel->setDstNode(expectedQueryNodeB.get());
    auto expectedQueryGraph = make_unique<QueryGraph>();
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeA));
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeB));
    expectedQueryGraph->addQueryRel(move(expectedQueryRel));
    auto expectedMatch = make_unique<BoundMatchStatement>(move(expectedQueryGraph));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:person)-[e1:knows]->(b:person);";

    auto binder = make_unique<Binder>(*catalog);
    auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
    ASSERT_TRUE(*boundStatements.at(0) == *expectedMatch);
}

TEST_F(QueryGraphTest, MultiEdgesTest) {
    auto expectedQueryNodeA = make_unique<QueryNode>("a", catalog->getNodeLabelFromString("person"));
    auto expectedQueryNodeB = make_unique<QueryNode>("_gfN_1", catalog->getNodeLabelFromString("person"));
    auto expectedQueryRel1 = make_unique<QueryRel>("e1", catalog->getRelLabelFromString("knows"));
    expectedQueryRel1->setSrcNode(expectedQueryNodeA.get());
    expectedQueryRel1->setDstNode(expectedQueryNodeB.get());
    auto expectedQueryNodeC = make_unique<QueryNode>("c", catalog->getNodeLabelFromString("comment"));
    auto expectedQueryRel2 = make_unique<QueryRel>("e2", catalog->getRelLabelFromString("likes"));
    expectedQueryRel2->setSrcNode(expectedQueryNodeB.get());
    expectedQueryRel2->setDstNode(expectedQueryNodeC.get());
    auto expectedQueryGraph = make_unique<QueryGraph>();
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeA));
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeB));
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeC));
    expectedQueryGraph->addQueryRel(move(expectedQueryRel1));
    expectedQueryGraph->addQueryRel(move(expectedQueryRel2));
    auto expectedMatch = make_unique<BoundMatchStatement>(move(expectedQueryGraph));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:person)-[e1:knows]->(:person)-[e2:likes]->(c:comment);";

    auto binder = make_unique<Binder>(*catalog);
    auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
    ASSERT_TRUE(*boundStatements.at(0) == *expectedMatch);
}

TEST_F(QueryGraphTest, MultiPatternElementsTest) {
    auto expectedQueryNodeA = make_unique<QueryNode>("a", catalog->getNodeLabelFromString("person"));
    auto expectedQueryNodeB = make_unique<QueryNode>("_gfN_1", catalog->getNodeLabelFromString("person"));
    auto expectedQueryRel1 = make_unique<QueryRel>("e1", catalog->getRelLabelFromString("knows"));
    expectedQueryRel1->setSrcNode(expectedQueryNodeA.get());
    expectedQueryRel1->setDstNode(expectedQueryNodeB.get());
    auto expectedQueryNodeC = make_unique<QueryNode>("c", catalog->getNodeLabelFromString("comment"));
    auto expectedQueryRel2 = make_unique<QueryRel>("e2", catalog->getRelLabelFromString("likes"));
    expectedQueryRel2->setSrcNode(expectedQueryNodeB.get());
    expectedQueryRel2->setDstNode(expectedQueryNodeC.get());
    auto expectedQueryNodeD = make_unique<QueryNode>("d", catalog->getNodeLabelFromString("person"));
    auto expectedQueryRel3 = make_unique<QueryRel>("_gfR_2", catalog->getRelLabelFromString("likes"));
    expectedQueryRel3->setSrcNode(expectedQueryNodeD.get());
    expectedQueryRel3->setDstNode(expectedQueryNodeC.get());
    auto expectedQueryGraph = make_unique<QueryGraph>();
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeA));
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeB));
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeC));
    expectedQueryGraph->addQueryNode(move(expectedQueryNodeD));
    expectedQueryGraph->addQueryRel(move(expectedQueryRel1));
    expectedQueryGraph->addQueryRel(move(expectedQueryRel2));
    expectedQueryGraph->addQueryRel(move(expectedQueryRel3));
    auto expectedMatch = make_unique<BoundMatchStatement>(move(expectedQueryGraph));

    graphflow::parser::Parser parser;
    string input = "MATCH (a:person)-[e1:knows]->(:person)-[e2:likes]->(c:comment), (c)<-[:likes]-(d:person);";

    auto binder = make_unique<Binder>(*catalog);
    auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
    ASSERT_TRUE(*boundStatements.at(0) == *expectedMatch);
}

TEST_F(QueryGraphTest, NonExistLabelTest) {
    EXPECT_THROW({
         graphflow::parser::Parser parser;
         string input = "MATCH (a:dummy);";
         auto binder = make_unique<Binder>(*catalog);
         auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
    }, invalid_argument);
}

TEST_F(QueryGraphTest, RepeatedNameTest) {
    EXPECT_THROW({
         graphflow::parser::Parser parser;
         string input = "MATCH (a:person)-[e1:knows]->(b:person)-[e1:knows]->(c:person);";
         auto binder = make_unique<Binder>(*catalog);
         auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
     }, invalid_argument);
}

TEST_F(QueryGraphTest, MultiLabelTest) {
    EXPECT_THROW({
         graphflow::parser::Parser parser;
         string input = "MATCH (a:person)-[e1:knows]->(b:person), (b:comment);";
         auto binder = make_unique<Binder>(*catalog);
         auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
     }, invalid_argument);
}

TEST_F(QueryGraphTest, NoMatchedRelTest) {
    EXPECT_THROW({
         graphflow::parser::Parser parser;
         string input = "MATCH (a:person)-[e1:likes]->(b:person);";
         auto binder = make_unique<Binder>(*catalog);
         auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
     }, invalid_argument);
}

TEST_F(QueryGraphTest, DisconnectGraphTest) {
    EXPECT_THROW({
         graphflow::parser::Parser parser;
         string input = "MATCH (a:person)-[e1:likes]->(b:person), (c:comment);";
         auto binder = make_unique<Binder>(*catalog);
         auto boundStatements = binder->bindSingleQuery(*parser.parseQuery(input));
     }, invalid_argument);
}
