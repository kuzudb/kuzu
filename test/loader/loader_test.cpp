#include "test/test_utility/include/test_helper.h"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/storage/include/data_structure/lists/unstructured_property_lists.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class LoaderNodePropertyTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/loader-node-property-test/"; }
};

class LoaderReadLists2BytesPerEdgeTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/2-bytes-per-edge/"; }
};

class LoaderReadLists3BytesPerEdgeTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/3-bytes-per-edge/"; }
};

class LoaderReadLists4BytesPerEdgeTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/4-bytes-per-edge/"; }
};

class LoaderReadLists5BytesPerEdgeTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/5-bytes-per-edge/"; }
};

struct KnowsLabelPLabelPKnowsLists {
    label_t knowsRelLabel;
    label_t pNodeLabel;
    AdjLists* fwdPKnowsLists;
    AdjLists* bwdPKnowsLists;
};

struct ALabelAKnowsLists {
    label_t aNodeLabel;
    AdjLists* fwdAKnowsLists;
    AdjLists* bwdAKnowsLists;
};

KnowsLabelPLabelPKnowsLists getKnowsLabelPLabelPKnowsLists(System* system) {
    KnowsLabelPLabelPKnowsLists retVal;
    auto& catalog = system->graph->getCatalog();
    retVal.pNodeLabel = catalog.getNodeLabelFromString("person");
    retVal.knowsRelLabel = catalog.getRelLabelFromString("knows");
    retVal.fwdPKnowsLists =
        system->graph->getRelsStore().getAdjLists(FWD, retVal.pNodeLabel, retVal.knowsRelLabel);
    retVal.bwdPKnowsLists =
        system->graph->getRelsStore().getAdjLists(BWD, retVal.pNodeLabel, retVal.knowsRelLabel);
    return retVal;
}

ALabelAKnowsLists getALabelAKnowsLists(System* system) {
    ALabelAKnowsLists retVal;
    auto& catalog = system->graph->getCatalog();
    retVal.aNodeLabel = catalog.getNodeLabelFromString("animal");
    auto knowsRelLabel = catalog.getRelLabelFromString("knows");
    retVal.fwdAKnowsLists =
        system->graph->getRelsStore().getAdjLists(FWD, retVal.aNodeLabel, knowsRelLabel);
    retVal.bwdAKnowsLists =
        system->graph->getRelsStore().getAdjLists(BWD, retVal.aNodeLabel, knowsRelLabel);
    return retVal;
}

TEST_F(LoaderNodePropertyTest, NodeStructuredStringPropertyTest) {
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto propertyIdx = catalog.getNodeProperty(label, "randomString");
    auto column = reinterpret_cast<StringPropertyColumn*>(
        defaultSystem->graph->getNodesStore().getNodePropertyColumn(label, propertyIdx.id));
    string fName = getInputCSVDir() + "vPerson.csv";
    CSVReader csvReader(fName, ',');
    int lineIdx = 0;
    csvReader.hasNextLine();
    csvReader.skipLine();
    NumericMetric numBufferHits(true), numBufferMisses(true), numIO(true);
    BufferManagerMetrics metrics(numBufferHits, numBufferMisses, numIO);
    while (csvReader.hasNextLine()) {
        csvReader.hasNextToken();
        csvReader.skipToken();
        csvReader.hasNextToken();
        EXPECT_EQ(string(csvReader.getString()), column->readValue(lineIdx, metrics).strVal);
        lineIdx++;
        csvReader.skipLine();
    }
}

TEST_F(LoaderNodePropertyTest, NodeUnstructuredPropertyTest) {
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto lists = reinterpret_cast<UnstructuredPropertyLists*>(
        defaultSystem->graph->getNodesStore().getNodeUnstrPropertyLists(label));
    auto& propertyNameToIdMap = catalog.getUnstrPropertiesNameToIdMap(label);
    for (int i = 0; i < 1000; ++i) {
        auto propertiesMap = lists->readUnstructuredPropertiesOfNode(i, *metrics);
        if (i == 300 || i == 400 || i == 500) {
            EXPECT_EQ(i * 4, propertiesMap->size());
            for (int j = 0; j < i; ++j) {
                EXPECT_EQ("strPropVal" + to_string(j),
                    propertiesMap->at(propertyNameToIdMap.at("strPropKey" + to_string(j))).strVal);
                EXPECT_EQ(
                    j, propertiesMap->at(propertyNameToIdMap.at("int64PropKey" + to_string(j)))
                           .val.int64Val);
                EXPECT_EQ(j * 1.0,
                    propertiesMap->at(propertyNameToIdMap.at("doublePropKey" + to_string(j)))
                        .val.doubleVal);
                EXPECT_FALSE(propertiesMap->at(propertyNameToIdMap.at("boolPropKey" + to_string(j)))
                                 .val.booleanVal);
            }
        } else {
            EXPECT_EQ(4, propertiesMap->size());
            EXPECT_EQ(
                "strPropVal1", propertiesMap->at(propertyNameToIdMap.at("strPropKey1")).strVal);
            EXPECT_EQ(1, propertiesMap->at(propertyNameToIdMap.at("int64PropKey1")).val.int64Val);
            EXPECT_EQ(
                1.0, propertiesMap->at(propertyNameToIdMap.at("doublePropKey1")).val.doubleVal);
            EXPECT_TRUE(propertiesMap->at(propertyNameToIdMap.at("boolPropKey1")).val.booleanVal);
        }
    }
}

void verifyP0ToP5999(
    KnowsLabelPLabelPKnowsLists& knowsLabelPLabelPKnowsLists, BufferManagerMetrics& metrics) {
    // p0 has 5001 fwd edges to p0...p5000
    node_offset_t p0Offset = 0;
    auto pOFwdList =
        knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(p0Offset, metrics);
    EXPECT_EQ(5001, pOFwdList->size());
    for (int nodeOffset = 0; nodeOffset <= 5000; ++nodeOffset) {
        nodeID_t nodeIDPi(nodeOffset, knowsLabelPLabelPKnowsLists.pNodeLabel);
        auto nbrNodeID = pOFwdList->at(nodeOffset);
        EXPECT_EQ(nodeIDPi, nbrNodeID);
    }
    // p0 has only 1 bwd edge, which from itself
    auto p0BwdList =
        knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(p0Offset, metrics);
    EXPECT_EQ(1, p0BwdList->size());
    nodeID_t p0NodeID(p0Offset, knowsLabelPLabelPKnowsLists.pNodeLabel);
    EXPECT_EQ(p0NodeID, pOFwdList->at(0));

    // p1,p2,...,p5000 have a single fwd edge to p5000 and 1 bwd edge from node p0
    nodeID_t nodeIDP5000(5000ul, knowsLabelPLabelPKnowsLists.pNodeLabel);
    for (node_offset_t nodeOffset = 1; nodeOffset <= 5000; ++nodeOffset) {
        auto fwdAdjList = knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(
            nodeOffset, metrics);
        EXPECT_EQ(1, fwdAdjList->size());
        EXPECT_EQ(nodeIDP5000, fwdAdjList->at(0));
        auto bwdAdjList = knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(
            nodeOffset, metrics);
        EXPECT_EQ(1, fwdAdjList->size());
        EXPECT_EQ(p0NodeID, bwdAdjList->at(0));
    }

    // p5001 to p6000 are singletons
    for (node_offset_t nodeOffset = 5001; nodeOffset < 6000; ++nodeOffset) {
        EXPECT_TRUE(
            knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(nodeOffset, metrics)
                ->empty());
        EXPECT_TRUE(
            knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(nodeOffset, metrics)
                ->empty());
    }
}

void verifya0Andp6000(KnowsLabelPLabelPKnowsLists& knowsLabelPLabelPKnowsLists,
    System* defaultSystem, BufferManagerMetrics& metrics) {
    auto aLabelAKnowsLists = getALabelAKnowsLists(defaultSystem);
    // a0 has 1 fwd edge to p6000, and no backward edges.
    node_offset_t a0NodeOffset = 0;
    node_offset_t p6000NodeOffset = 6000;
    auto a0FwdList =
        aLabelAKnowsLists.fwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset, metrics);
    EXPECT_EQ(1, a0FwdList->size());
    nodeID_t p6000NodeID(p6000NodeOffset, knowsLabelPLabelPKnowsLists.pNodeLabel);
    EXPECT_EQ(p6000NodeID, a0FwdList->at(0));
    auto a0BwdList =
        aLabelAKnowsLists.bwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset, metrics);
    EXPECT_TRUE(
        aLabelAKnowsLists.bwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset, metrics)->empty());

    // p6000 has no fwd edges and 1 bwd edge from a0
    EXPECT_TRUE(knowsLabelPLabelPKnowsLists.fwdPKnowsLists
                    ->readAdjacencyListOfNode(p6000NodeOffset, metrics)
                    ->empty());
    auto p6000BwdList = knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(
        p6000NodeOffset, metrics);
    nodeID_t a0NodeID(a0NodeOffset, aLabelAKnowsLists.aNodeLabel);
    EXPECT_EQ(1, p6000BwdList->size());
    EXPECT_EQ(a0NodeID, p6000BwdList->at(0));
}

void verifyP6001ToP65999(
    KnowsLabelPLabelPKnowsLists& knowsLabelPLabelPKnowsLists, BufferManagerMetrics& metrics) {
    for (node_offset_t node_offset_t = 6001; node_offset_t < 66000; ++node_offset_t) {
        EXPECT_TRUE(knowsLabelPLabelPKnowsLists.fwdPKnowsLists
                        ->readAdjacencyListOfNode(node_offset_t, metrics)
                        ->empty());
        EXPECT_TRUE(knowsLabelPLabelPKnowsLists.bwdPKnowsLists
                        ->readAdjacencyListOfNode(node_offset_t, metrics)
                        ->empty());
    }
}

TEST_F(LoaderReadLists2BytesPerEdgeTest, ReadLists2BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists = getKnowsLabelPLabelPKnowsLists(defaultSystem.get());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists, *metrics.get());
}

TEST_F(LoaderReadLists3BytesPerEdgeTest, ReadLists3BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists = getKnowsLabelPLabelPKnowsLists(defaultSystem.get());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists, *metrics.get());
    verifya0Andp6000(knowsLabelPLabelPKnowsLists, defaultSystem.get(), *metrics.get());
}

TEST_F(LoaderReadLists4BytesPerEdgeTest, ReadLists4BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists = getKnowsLabelPLabelPKnowsLists(defaultSystem.get());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists, *metrics.get());
    verifyP6001ToP65999(knowsLabelPLabelPKnowsLists, *metrics.get());
}

TEST_F(LoaderReadLists5BytesPerEdgeTest, ReadLists5BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists = getKnowsLabelPLabelPKnowsLists(defaultSystem.get());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists, *metrics.get());
    verifya0Andp6000(knowsLabelPLabelPKnowsLists, defaultSystem.get(), *metrics.get());
    verifyP6001ToP65999(knowsLabelPLabelPKnowsLists, *metrics.get());
}
