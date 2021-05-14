#include "gtest/gtest.h"
#include "mock_catalog.h"
#include "mock_graph.h"

#include "src/parser/include/parser.h"
#include "src/planner/include/binder.h"
#include "src/planner/include/enumerator.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::StrEq;
using ::testing::Test;
using ::testing::Throw;

using namespace graphflow::planner;

/**
 * Mock person-knows-person catalog with 1 node label person
 * and 1 rel label knows. Person has property age with type INT32.
 * Knows has property description with type STRING.
 */
class PersonKnowsPersonCatalog : public MockCatalog {

public:
    void setUp() {
        setSrcNodeLabelToRelLabels();
        setDstNodeLabelToRelLabels();

        setActionForContainNodeLabel();
        setActionForGetNodeLabelFromString();
        setActionForContainRelLabel();
        setActionForGetRelLabelFromString();
        setActionForGetRelLabelsForNodeLabelDirection();
        setActionForContainNodeProperty();
        setActionForGetNodePropertyTypeFromString();
        setActionForContainUnstrNodeProperty();
        setActionForContainRelProperty();
        setActionForGetRelPropertyTypeFromString();
        setActionForIsSingleCardinalityInDir();
    }

private:
    void setActionForContainNodeLabel() {
        ON_CALL(*this, containNodeLabel(_)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeLabel(StrEq("person"))).WillByDefault(Return(true));
    }

    void setActionForGetNodeLabelFromString() {
        ON_CALL(*this, getNodeLabelFromString(StrEq("person"))).WillByDefault(Return(0));
    }

    void setActionForContainRelLabel() {
        ON_CALL(*this, containRelLabel(_)).WillByDefault(Return(false));
        ON_CALL(*this, containRelLabel(StrEq("knows"))).WillByDefault(Return(true));
    }

    void setActionForGetRelLabelFromString() {
        ON_CALL(*this, getRelLabelFromString(StrEq("knows"))).WillByDefault(Return(0));
    }

    void setActionForGetRelLabelsForNodeLabelDirection() {
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(0, FWD))
            .WillByDefault(ReturnRef(srcNodeLabelToRelLabels[0]));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(0, BWD))
            .WillByDefault(ReturnRef(dstNodeLabelToRelLabels[0]));
    }

    void setActionForContainNodeProperty() {
        ON_CALL(*this, containNodeProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containNodeProperty(0, _)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeProperty(0, "age")).WillByDefault(Return(true));
    }

    void setActionForGetNodePropertyTypeFromString() {
        ON_CALL(*this, getNodePropertyTypeFromString(0, "age")).WillByDefault(Return(INT32));
    }

    void setActionForContainUnstrNodeProperty() {
        ON_CALL(*this, containUnstrNodeProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containUnstrNodeProperty(0, _)).WillByDefault(Return(false));
    }

    void setActionForContainRelProperty() {
        ON_CALL(*this, containRelProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containRelProperty(0, _)).WillByDefault(Return(false));
        ON_CALL(*this, containRelProperty(0, "description")).WillByDefault(Return(true));
    }

    void setActionForGetRelPropertyTypeFromString() {
        ON_CALL(*this, getRelPropertyTypeFromString(0, "description"))
            .WillByDefault(Return(STRING));
    }

    void setActionForIsSingleCardinalityInDir() {
        ON_CALL(*this, isSingleCaridinalityInDir(_, _)).WillByDefault(Return(false));
    }

    void setSrcNodeLabelToRelLabels() {
        vector<label_t> personToRelLabels = {0};
        srcNodeLabelToRelLabels.push_back(move(personToRelLabels));
    }

    void setDstNodeLabelToRelLabels() {
        vector<label_t> personToRelLabels = {0};
        dstNodeLabelToRelLabels.push_back(move(personToRelLabels));
    }

    vector<vector<label_t>> srcNodeLabelToRelLabels, dstNodeLabelToRelLabels;
};

/**
 * Mock person-knows-person graph with 10000 person nodes
 * fwd average degree 10, bwd average degree 20
 */
class PersonKnowsPersonGraph : public MockGraph {

public:
    void setUp() {
        numPersonNodes = 10000;
        setCatalog();

        setActionForGetCatalog();
        setActionForGetNumNodes();
        setActionForGetNumRelsForDirBoundLabelRelLabel();
    }

private:
    void setActionForGetCatalog() { ON_CALL(*this, getCatalog).WillByDefault(ReturnRef(*catalog)); }

    void setActionForGetNumNodes() {
        ON_CALL(*this, getNumNodes(_))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getNumNodes(0)).WillByDefault(Return(numPersonNodes));
    }

    void setActionForGetNumRelsForDirBoundLabelRelLabel() {
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(_, _, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(FWD, 0, 0))
            .WillByDefault(Return(10 * numPersonNodes));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(BWD, 0, 0))
            .WillByDefault(Return(20 * numPersonNodes));
    }

    void setCatalog() {
        auto mockCatalog = make_unique<NiceMock<PersonKnowsPersonCatalog>>();
        mockCatalog->setUp();
        catalog = move(mockCatalog);
    }

    unique_ptr<Catalog> catalog;
    uint64_t numPersonNodes;
};

class OptimizerTest : public Test {

public:
    static unique_ptr<LogicalPlan> getBestPlan(const string& query, const Graph& graph) {
        auto parsedQuery = Parser::parseQuery(query);
        auto boundQuery = Binder(graph.getCatalog()).bindSingleQuery(*parsedQuery);
        return Enumerator(graph, *boundQuery).getBestPlan();
    }

    static bool containSubstr(const string& str, const string& substr) {
        return str.find(substr) != string::npos;
    }
};

TEST_F(OptimizerTest, OneHopSingleFilter) {
    NiceMock<PersonKnowsPersonGraph> graph;
    graph.setUp();

    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age = 1 RETURN COUNT(*)";
    auto plan = OptimizerTest::getBestPlan(query, graph);
    auto& op1 = plan->getLastOperator();
    ASSERT_EQ(LOGICAL_EXTEND, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalExtend&)op1).nbrNodeID, "_b." + INTERNAL_ID));
    auto& op2 = *op1.prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op2.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op2).nodeID, "_a." + INTERNAL_ID));
}

TEST_F(OptimizerTest, OneHopMultiFilters) {
    NiceMock<PersonKnowsPersonGraph> graph;
    graph.setUp();

    auto query = "MATCH (a:person)-[:knows]->(b:person) WHERE a.age > 10 AND a.age < 20 AND b.age "
                 "= 45 RETURN COUNT(*)";
    auto plan = OptimizerTest::getBestPlan(query, graph);
    auto& op1 = *plan->getLastOperator().prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_EXTEND, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalExtend&)op1).nbrNodeID, "_b." + INTERNAL_ID));
    auto& op2 = *op1.prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op2.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op2).nodeID, "_a." + INTERNAL_ID));
}

TEST_F(OptimizerTest, TwoHop) {
    NiceMock<PersonKnowsPersonGraph> graph;
    graph.setUp();

    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) RETURN COUNT(*)";
    auto plan = OptimizerTest::getBestPlan(query, graph);
    auto& op1 = *plan->getLastOperator().prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op1).nodeID, "_b." + INTERNAL_ID));
}

TEST_F(OptimizerTest, TwoHopMultiFilters) {
    NiceMock<PersonKnowsPersonGraph> graph;
    graph.setUp();

    auto query = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) WHERE a.age = 20 AND "
                 "b.age = 35 RETURN COUNT(*)";
    auto plan = OptimizerTest::getBestPlan(query, graph);
    auto& op1 =
        *plan->getLastOperator()
             .prevOperator->prevOperator->prevOperator->prevOperator->prevOperator->prevOperator;
    ASSERT_EQ(LOGICAL_SCAN_NODE_ID, op1.getLogicalOperatorType());
    ASSERT_TRUE(containSubstr(((LogicalScanNodeID&)op1).nodeID, "_b." + INTERNAL_ID));
}
