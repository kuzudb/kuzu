#include "test/test_utility/include/test_helper.h"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::catalog;
using namespace graphflow::loader;
using namespace graphflow::storage;
using namespace graphflow::testing;

namespace graphflow {
namespace loader {

class LoaderNodePropertyTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/loader-node-property-test/"; }
};

class LoaderSpecialCharTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/loader-special-char-test/"; }
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

class LoaderEmptyListsTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/loader-empty-lists-test/"; }
    // The test is here because accessing protected/private members of Lists and ListsMetadata
    // requires the code to be inside LoaderEmptyListsTest class, which is a friend class to
    // Lists and ListsMetadata.
    void testLoaderEmptyListsTest() {
        auto& catalog = *database->getCatalog();
        label_t pLabel = catalog.getReadOnlyVersion()->getNodeLabelFromName("person");
        auto unstrPropLists =
            database->getStorageManager()->getNodesStore().getNodeUnstrPropertyLists(pLabel);
        unstrPropLists->metadata.chunkToPageListHeadIdxMap->header.print();
        unstrPropLists->metadata.largeListIdxToPageListHeadIdxMap->header.print();
        unstrPropLists->metadata.pageLists->header.print();
        // The vPerson table has 4 chunks (2000/512) and only nodeOffset=1030, which is in chunk idx
        // 2 has a non-empty list. So chunk ids 0, 1, and 3's chunkToPageListHeadIdxMap need to
        // point to UINT32_MAX (representing null), while chunk 2 should point to 0.
        uint64_t numChunks = 4;
        EXPECT_EQ(
            numChunks, unstrPropLists->metadata.chunkToPageListHeadIdxMap->header.numElements);
        for (int chunkIdx = 0; chunkIdx < numChunks; chunkIdx++) {
            EXPECT_EQ(chunkIdx == 2 ? 0 : UINT32_MAX,
                (*unstrPropLists->metadata.chunkToPageListHeadIdxMap)[chunkIdx]);
        }
        // Check chunk idx 2's pageLists.
        EXPECT_EQ(storage::ListsMetadataConfig::PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE,
            unstrPropLists->metadata.pageLists->header.numElements);
        for (int chunkPageListIdx = 0;
             chunkPageListIdx < storage::ListsMetadataConfig::PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE;
             ++chunkPageListIdx) {
            if (chunkPageListIdx == 0) {
                EXPECT_NE(PAGE_IDX_MAX, (*unstrPropLists->metadata.pageLists)[chunkPageListIdx]);
            } else {
                EXPECT_EQ(PAGE_IDX_MAX, (*unstrPropLists->metadata.pageLists)[chunkPageListIdx]);
            }
        }
        // There are no large lists so largeListIdxToPageListHeadIdxMap should have 0 elements.
        EXPECT_EQ(0, unstrPropLists->metadata.largeListIdxToPageListHeadIdxMap->header.numElements);
        uint64_t maxPersonOffset = database->getStorageManager()
                                       ->getNodesStore()
                                       .getNodesMetadata()
                                       .getNodeMetadata(pLabel)
                                       ->getMaxNodeOffset();
        EXPECT_EQ(1999, maxPersonOffset);
        for (node_offset_t nodeOffset = 0; nodeOffset < maxPersonOffset; ++nodeOffset) {
            auto unstructuredProperties =
                unstrPropLists->readUnstructuredPropertiesOfNode(nodeOffset);
            EXPECT_EQ((1030 == nodeOffset) ? 1 : 0, unstructuredProperties->size());
        }
    }
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

KnowsLabelPLabelPKnowsLists getKnowsLabelPLabelPKnowsLists(
    const Catalog& catalog, StorageManager* graph) {
    KnowsLabelPLabelPKnowsLists retVal;
    retVal.pNodeLabel = catalog.getReadOnlyVersion()->getNodeLabelFromName("person");
    retVal.knowsRelLabel = catalog.getReadOnlyVersion()->getRelLabelFromName("knows");
    retVal.fwdPKnowsLists =
        graph->getRelsStore().getAdjLists(FWD, retVal.pNodeLabel, retVal.knowsRelLabel);
    retVal.bwdPKnowsLists =
        graph->getRelsStore().getAdjLists(BWD, retVal.pNodeLabel, retVal.knowsRelLabel);
    return retVal;
}

ALabelAKnowsLists getALabelAKnowsLists(const Catalog& catalog, StorageManager* storageManager) {
    ALabelAKnowsLists retVal;
    retVal.aNodeLabel = catalog.getReadOnlyVersion()->getNodeLabelFromName("animal");
    auto knowsRelLabel = catalog.getReadOnlyVersion()->getRelLabelFromName("knows");
    retVal.fwdAKnowsLists =
        storageManager->getRelsStore().getAdjLists(FWD, retVal.aNodeLabel, knowsRelLabel);
    retVal.bwdAKnowsLists =
        storageManager->getRelsStore().getAdjLists(BWD, retVal.aNodeLabel, knowsRelLabel);
    return retVal;
}
} // namespace loader
} // namespace graphflow

TEST_F(LoaderNodePropertyTest, NodeStructuredStringPropertyTest) {
    auto graph = database->getStorageManager();
    auto& catalog = *database->getCatalog();
    auto label = catalog.getReadOnlyVersion()->getNodeLabelFromName("person");
    auto propertyIdx = catalog.getReadOnlyVersion()->getNodeProperty(label, "randomString");
    auto column = reinterpret_cast<StringPropertyColumn*>(
        graph->getNodesStore().getNodePropertyColumn(label, propertyIdx.propertyID));
    string fName = getInputCSVDir() + "vPerson.csv";
    CSVReaderConfig config;
    CSVReader csvReader(fName, config);
    int lineIdx = 0;
    csvReader.hasNextLine();
    csvReader.skipLine();
    uint64_t count = 0;
    while (csvReader.hasNextLine()) {
        csvReader.hasNextToken();
        csvReader.skipToken();
        csvReader.hasNextToken();
        if ((count % 100) == 0) {
            ASSERT_TRUE(column->isNull(count /* nodeOffset */));
        } else {
            ASSERT_FALSE(column->isNull(count /* nodeOffset */));
            EXPECT_EQ(string(csvReader.getString()), column->readValue(lineIdx).strVal);
        }
        lineIdx++;
        csvReader.skipLine();
        count++;
    }
}

TEST_F(LoaderNodePropertyTest, NodeUnstructuredPropertyTest) {
    auto graph = database->getStorageManager();
    auto& catalog = *database->getCatalog();
    auto label = catalog.getReadOnlyVersion()->getNodeLabelFromName("person");
    auto lists = reinterpret_cast<UnstructuredPropertyLists*>(
        graph->getNodesStore().getNodeUnstrPropertyLists(label));
    auto& propertyNameToIdMap = catalog.getReadOnlyVersion()->getUnstrPropertiesNameToIdMap(label);
    for (int i = 0; i < 1000; ++i) {
        auto propertiesMap = lists->readUnstructuredPropertiesOfNode(i);
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

void verifyP0ToP5999(KnowsLabelPLabelPKnowsLists& knowsLabelPLabelPKnowsLists) {
    // p0 has 5001 fwd edges to p0...p5000
    node_offset_t p0Offset = 0;
    auto pOFwdList = knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(p0Offset);
    EXPECT_EQ(5001, pOFwdList->size());
    for (int nodeOffset = 0; nodeOffset <= 5000; ++nodeOffset) {
        nodeID_t nodeIDPi(nodeOffset, knowsLabelPLabelPKnowsLists.pNodeLabel);
        auto nbrNodeID = pOFwdList->at(nodeOffset);
        EXPECT_EQ(nodeIDPi, nbrNodeID);
    }
    // p0 has only 1 bwd edge, which from itself
    auto p0BwdList = knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(p0Offset);
    EXPECT_EQ(1, p0BwdList->size());
    nodeID_t p0NodeID(p0Offset, knowsLabelPLabelPKnowsLists.pNodeLabel);
    EXPECT_EQ(p0NodeID, pOFwdList->at(0));

    // p1,p2,...,p5000 have a single fwd edge to p5000 and 1 bwd edge from node p0
    nodeID_t nodeIDP5000(5000ul, knowsLabelPLabelPKnowsLists.pNodeLabel);
    for (node_offset_t nodeOffset = 1; nodeOffset <= 5000; ++nodeOffset) {
        auto fwdAdjList =
            knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(nodeOffset);
        EXPECT_EQ(1, fwdAdjList->size());
        EXPECT_EQ(nodeIDP5000, fwdAdjList->at(0));
        auto bwdAdjList =
            knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(nodeOffset);
        EXPECT_EQ(1, fwdAdjList->size());
        EXPECT_EQ(p0NodeID, bwdAdjList->at(0));
    }

    // p5001 to p6000 are singletons
    for (node_offset_t nodeOffset = 5001; nodeOffset < 6000; ++nodeOffset) {
        EXPECT_TRUE(knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(nodeOffset)
                        ->empty());
        EXPECT_TRUE(knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(nodeOffset)
                        ->empty());
    }
}

void verifya0Andp6000(KnowsLabelPLabelPKnowsLists& knowsLabelPLabelPKnowsLists,
    const Catalog& catalog, StorageManager* storageManager) {
    auto aLabelAKnowsLists = getALabelAKnowsLists(catalog, storageManager);
    // a0 has 1 fwd edge to p6000, and no backward edges.
    node_offset_t a0NodeOffset = 0;
    node_offset_t p6000NodeOffset = 6000;
    auto a0FwdList = aLabelAKnowsLists.fwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset);
    EXPECT_EQ(1, a0FwdList->size());
    nodeID_t p6000NodeID(p6000NodeOffset, knowsLabelPLabelPKnowsLists.pNodeLabel);
    EXPECT_EQ(p6000NodeID, a0FwdList->at(0));
    auto a0BwdList = aLabelAKnowsLists.bwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset);
    EXPECT_TRUE(aLabelAKnowsLists.bwdAKnowsLists->readAdjacencyListOfNode(a0NodeOffset)->empty());

    // p6000 has no fwd edges and 1 bwd edge from a0
    EXPECT_TRUE(knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(p6000NodeOffset)
                    ->empty());
    auto p6000BwdList =
        knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(p6000NodeOffset);
    nodeID_t a0NodeID(a0NodeOffset, aLabelAKnowsLists.aNodeLabel);
    EXPECT_EQ(1, p6000BwdList->size());
    EXPECT_EQ(a0NodeID, p6000BwdList->at(0));
}

void verifyP6001ToP65999(KnowsLabelPLabelPKnowsLists& knowsLabelPLabelPKnowsLists) {
    for (node_offset_t node_offset_t = 6001; node_offset_t < 66000; ++node_offset_t) {
        EXPECT_TRUE(
            knowsLabelPLabelPKnowsLists.fwdPKnowsLists->readAdjacencyListOfNode(node_offset_t)
                ->empty());
        EXPECT_TRUE(
            knowsLabelPLabelPKnowsLists.bwdPKnowsLists->readAdjacencyListOfNode(node_offset_t)
                ->empty());
    }
}

TEST_F(LoaderReadLists2BytesPerEdgeTest, ReadLists2BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists =
        getKnowsLabelPLabelPKnowsLists(*database->getCatalog(), database->getStorageManager());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists);
}

TEST_F(LoaderReadLists3BytesPerEdgeTest, ReadLists3BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists =
        getKnowsLabelPLabelPKnowsLists(*database->getCatalog(), database->getStorageManager());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists);
    verifya0Andp6000(
        knowsLabelPLabelPKnowsLists, *database->getCatalog(), database->getStorageManager());
}

TEST_F(LoaderReadLists4BytesPerEdgeTest, ReadLists4BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists =
        getKnowsLabelPLabelPKnowsLists(*database->getCatalog(), database->getStorageManager());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists);
    verifyP6001ToP65999(knowsLabelPLabelPKnowsLists);
}

TEST_F(LoaderReadLists5BytesPerEdgeTest, ReadLists5BytesPerEdgeTest) {
    auto knowsLabelPLabelPKnowsLists =
        getKnowsLabelPLabelPKnowsLists(*database->getCatalog(), database->getStorageManager());
    verifyP0ToP5999(knowsLabelPLabelPKnowsLists);
    verifya0Andp6000(
        knowsLabelPLabelPKnowsLists, *database->getCatalog(), database->getStorageManager());
    verifyP6001ToP65999(knowsLabelPLabelPKnowsLists);
}

TEST_F(LoaderSpecialCharTest, LoaderSpecialCharsCsv) {
    auto storageManager = database->getStorageManager();
    auto& catalog = *database->getCatalog();
    auto label = catalog.getReadOnlyVersion()->getNodeLabelFromName("person");
    auto propertyIdx = catalog.getReadOnlyVersion()->getNodeProperty(label, "randomString");
    auto col = storageManager->getNodesStore().getNodePropertyColumn(label, propertyIdx.propertyID);

    EXPECT_EQ("this is |the first line", col->readValue(0).strVal);
    EXPECT_EQ("the \" should be ignored", col->readValue(1).strVal);
    EXPECT_EQ("the - should be escaped", col->readValue(2).strVal);
    EXPECT_EQ("this -is #a mixed test", col->readValue(3).strVal);
    EXPECT_EQ("only one # should be recognized", col->readValue(4).strVal);
    EXPECT_EQ("this is a ##plain## #string", col->readValue(5).strVal);
    EXPECT_EQ("this is another ##plain## #string with \\", col->readValue(6).strVal);

    label = catalog.getReadOnlyVersion()->getNodeLabelFromName("organisation");
    propertyIdx = catalog.getReadOnlyVersion()->getNodeProperty(label, "name");
    col = storageManager->getNodesStore().getNodePropertyColumn(label, propertyIdx.propertyID);
    EXPECT_EQ("ABFsUni", col->readValue(0).strVal);
    EXPECT_EQ("CsW,ork", col->readValue(1).strVal);
    EXPECT_EQ("DEsW#ork", col->readValue(2).strVal);
}

TEST_F(LoaderEmptyListsTest, LoaderEmptyLists) {
    testLoaderEmptyListsTest();
}
